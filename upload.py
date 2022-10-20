"""
Program for uploading files to Google Drive.
Run with --help for more info.

Raphael Pithan
2021

TODO: create initial folder on drive if it doesn't exist
"""

import re
import os
import os.path
import sys
import time
import argparse
import signal
import threading
import tkinter as tk
from tkinter import filedialog
from tkinter import simpledialog
from datetime import datetime

from auxiliar import *
from drive import *
from work_queue import Dispatcher, Worker
from concurrent_progress_bar import ConcurrentProgressBar as ProgressBar

#===============================================================================
# Constants

# Configurable ----

MAX_CONCURRENT_UPLOADS = 4
LAST_EXECUTION_LOG = 'log/run%s.log'
DONT_UPLOAD_EXTENSIONS = [
    '.db', '.py', '.bat'
]

# Not configurable
GIGA = 1 * 1024 * 1024 * 1024

# Debug -----------

DEBUG_SKIP_CONFIRMATION = 0
DEBUG_DRY_RUN = False

# Log -------------

try:
    os.makedirs(os.path.split(LAST_EXECUTION_LOG)[0])
except:
    pass
f = open(LAST_EXECUTION_LOG % datetime.now().strftime("%Y%m%d%H%M%S%f"), 'wt')
def print2(*args, **kwargs):
    __builtins__.print(*args, **kwargs)
    endl = '\n' if not 'end' in kwargs else kwargs['end']
    f.write('\t'.join(args))
    f.write(endl)
print = print2

#===============================================================================
# User interface

"""
Find the most probable client secret file around.
"""
def get_client_secret_file():
    candidates = [f for f in os.listdir('.') if f.startswith('client_secret')]
    return safe_get_field(candidates, 0) # ooo, advanced, machine learning, AI logic

# Global
g_tk_root = None
 
"""
Open a file dialog for selecting a directory.
"""
def ask_for_source(initial=None):
    global g_tk_root
    if not g_tk_root:
        g_tk_root = tk.Tk()
        g_tk_root.withdraw()
    return filedialog.askdirectory(title='Source directory to upload',
        initialdir=initial)

"""
Open a text dialog prompt for typing the name of the destination directory.
"""
def ask_for_dest(initial):
    global g_tk_root
    if not g_tk_root:
        g_tk_root = tk.Tk()
        g_tk_root.withdraw()
    title = ''
    return simpledialog.askstring("Destination",
        'Destination directory on Google Drive (must exist):',
        initialvalue=initial)

"""
Check if directory should be excluded from uploads (looks at the parents too)
"""
def check_dir_excluded(path, options, root=None):
    if options['exclude_dir'] and len(options['exclude_dir']) > 0:
        excl = options['exclude_dir'].lower()
        if root:
            path = make_relative_path(path, root)
        dirs = path.split('/')
        for dir in dirs:            
            if dir.lower().find(excl) != -1:
                return True
    return False

"""
Extracts bytes value from human size (100M, 100MB, 1G, etc)
"""
def process_human_size(size):
    if size and len(size) > 0:
        size = size.upper()
        match = re.match('^(\d+)([KMG])?B?$', size)
        if match:
            base = int(match.group(1))
            unit = match.group(2)
            mult = unit and pow(1024, ['K', 'M', 'G'].index(unit) + 1) or 1
            return base * mult
    return None

#===============================================================================
# Main

# Global
g_stop_loop = False
g_progress_bar = None
g_thread_data = []

"""
Handle Ctrl+C during the main loop.
"""
def signal_handler(sig, frame):
    global g_stop_loop
    g_stop_loop = True
    print('Ctrl+C')

"""
Pretend to upload a file when dry run is configured.
"""
def debug_pretend_upload(file, callback):
    total = os.path.getsize(file)
    rate = 512 * 1024
    uploaded = 0
    while uploaded < total:
        time.sleep(0.5)
        uploaded += rate
        callback(min(uploaded, total), total)

"""
Main. See script's doc bellow for more information.
"""
def main(source_root, dest_root, options):
    # Examine source files
    if not os.path.exists(source_root):
        print('Source directory does not exist')
        sys.exit(1)
    
    print('Counting source files... ', end='')
    num_source_files = 0
    for path, dirs, files in os.walk(source_root):
        if not check_dir_excluded(path, options, root=source_root):
            num_source_files += len(files)
    print('%d files found' % num_source_files)
    if num_source_files == 0:
        sys.exit(0)
    
    # Examine destination
    secret_file = get_client_secret_file()
    if not secret_file:
        print('No client secret file found!')
        sys.exit(1)
    print('Connecting to Google Drive... ', end='')
    drive = Drive()
    drive.connect(secret_file)
    print('CONNECTED')
    
    print('Searching for the destination path in your Drive... ', end='')
    dest_root_id = drive.get_path(dest_root)
    if not dest_root_id:
        print('\nERROR: path "%s" not found in your Drive' % dest_root)
        sys.exit(1)
    print('FOUND (%s)' % dest_root_id)
    
    # Show confirmation
    print('\n--The following operation will be executed--')
    print('Copy up to %d files from\n  >>>"%s"<<<' % (num_source_files, source_root))
    print('to your Google Drive path\n  >>>"%s"<<<.' % dest_root)
    print('Existing files will be skipped.')
    if DEBUG_DRY_RUN:
        print('## THIS IS A DRY RUN, NO UPLOADS WILL BE MADE ##')
    print('Operation can be interrupted at any time by >>>Ctrl+C<<<.')
    if not DEBUG_SKIP_CONFIRMATION and not options['skip_confirmation'] and input('Are you sure [y/N]? ').upper() != 'Y':
        print('Operation aborted!')
        sys.exit(1)
    
    # --- It's show time ---
    print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    
    global g_stop_loop
    signal.signal(signal.SIGINT, signal_handler)
    
    global g_progress_bar
    g_progress_bar = ProgressBar(MAX_CONCURRENT_UPLOADS)
    g_progress_bar.start()

    shared_data = {
        'lock': threading.Lock(),
        'size_uploaded_files': 0,
        'num_uploaded_files': 0,
        'num_upload_errors': 0,
        'num_existing_files': 0,
        'num_skipped_files': 0,
        'num_processed_files': 0,
        'error_streak': 0,
    }
    ERROR_STREAK_WAIT = 5
    ERROR_STREAK_ABORT = 10
    
    # Work queue
    def upload_task(data):
        shared_data = data['shared_data']
        file_data = data['file_data']
        tid = Worker.current_thread_id()
        my_drive = g_thread_data[tid]['drive']
        
        g_progress_bar.clear()
        print('[%d] uploading file "%s/%s" (%s)' % (tid, file_data['dest_path'],
            file_data['file'], format_pretty_size(file_data['file_size'])))
        g_progress_bar.redraw()
        
        try:
            callback = lambda progress, total: (g_progress_bar.update_part(tid, progress, total),
                g_progress_bar.update_total(shared_data['num_processed_files'], num_source_files))
                
            if not DEBUG_DRY_RUN:
                my_drive.upload_file(file_data['current_dest_id'], file_data['full_file_path'],
                    progress_callback=callback, check_exists=False, replace=options['replace'])
            else:
                debug_pretend_upload(file_data['full_file_path'], callback)
                
            with shared_data['lock']:
                shared_data['size_uploaded_files'] += file_data['file_size']
                shared_data['num_uploaded_files'] += 1
                shared_data['num_processed_files'] += 1
                shared_data['error_streak'] = 0
        except Exception as e:
            print('**File upload error: ' + str(e))
            with shared_data['lock']:
                shared_data['num_upload_errors'] += 1
                shared_data['num_processed_files'] += 1
                shared_data['error_streak'] += 1

    # Prepare and start worker threads
    for i in range(MAX_CONCURRENT_UPLOADS):
        g_thread_data.append({ 'drive': drive.duplicate_service() })
    queue = Dispatcher(MAX_CONCURRENT_UPLOADS, 2*MAX_CONCURRENT_UPLOADS, upload_task)
    queue.start()
    
    # Walk each subdir in source (including the root)
    start_time = time.time()
    for path, dirs, files in os.walk(source_root):
        if check_dir_excluded(path, options, root=source_root):
            print('Directory "%s" excluded from upload' % path)
            continue
                
        # Calculate the destination path for this directory in the Drive and
        # obtain the list of files that already exist there (as a hash map)
        relative_path = make_relative_path(path, source_root)
        dest_path = clean_path(dest_root + '/' + relative_path)
        current_dest_id = drive.ensure_path(dest_path)
        print('Listing files for "%s"...' % dest_path)
        existing_files_map = result_list_to_map(drive.list_files(current_dest_id))
        
        # Walk each file in this subdir
        for file in files:
            if not file in existing_files_map or options['replace']:
                # File does not exist in destination, upload it
                short_file_name = relative_path + '/' + file
                full_file_path = clean_path(path + '/' + file)
                file_size = os.path.getsize(full_file_path)
                if os.path.splitext(full_file_path)[-1] in DONT_UPLOAD_EXTENSIONS:
                    print('File "%s" not uploaded due to prevented extension' % short_file_name)
                    with shared_data['lock']:
                        shared_data['num_skipped_files'] += 1
                        shared_data['num_processed_files'] += 1
                elif options['max_size'] and file_size > options['max_size']:
                    print('File "%s" not uploaded due to size (%s)' % (short_file_name,
                        format_pretty_size(file_size)))
                    with shared_data['lock']:
                        shared_data['num_skipped_files'] += 1
                        shared_data['num_processed_files'] += 1
                else:
                    data = {
                        'shared_data': shared_data,
                        'file_data': {
                            'dest_path': dest_path,
                            'current_dest_id': current_dest_id,
                            'file': file,
                            'full_file_path': full_file_path,
                            'file_size': file_size,
                        },
                    }
                    if MAX_CONCURRENT_UPLOADS > 1:
                        while True:
                            if queue.add_data(data):
                                break
                            else:
                                time.sleep(0.1)
                    else:
                        upload_task(data)
                        
                    # Error handling
                    if shared_data['error_streak'] >= ERROR_STREAK_ABORT:
                        g_stop_loop = True
                    elif shared_data['error_streak'] >= ERROR_STREAK_WAIT:
                        print('Too many sequential errors, waiting 30 seconds before continuing...')
                        time.sleep(30)
            else:
                # File already exists in destination
                with shared_data['lock']:
                    shared_data['num_existing_files'] += 1
                    shared_data['num_processed_files'] += 1
            
            if g_stop_loop:
                queue.clear_data()
                break # for file
        if g_stop_loop:
            queue.clear_data()
            break # for path

    # Wait for the queue to become empty and the workers idle
    while True:
        if queue.has_data() or queue.is_busy():
            time.sleep(1)
        else:
            break
    queue.stop()

    end_time = time.time()
    elapsed_time = end_time - start_time
    
    g_progress_bar.stop()
    g_progress_bar.clear()
    
    # Show final statistics
    print('\n--Operation completed--')
    print('Time taken: ' + format_pretty_time(elapsed_time))
    print('Average upload speed:  %s/s | %d file(s)/min' % (
        format_pretty_size(shared_data['size_uploaded_files'] / elapsed_time),
        round(shared_data['num_uploaded_files'] * 60 / elapsed_time)))
    if shared_data['size_uploaded_files'] != 0:
        print('Time to 1 GB: %s' % format_pretty_time(GIGA * elapsed_time / shared_data['size_uploaded_files']))
    print('%d file(s) uploaded (%s)' % (
        shared_data['num_uploaded_files'],
        format_pretty_size(shared_data['size_uploaded_files'])))
    print('%d file(s) failed to upload' % shared_data['num_upload_errors'])
    if shared_data['num_skipped_files'] > 0:
        print('%d file(s) skipped' % shared_data['num_skipped_files'])
    print('%d file(s) already existed' % shared_data['num_existing_files'])
    print('')

USAGE = """
python upload.py [OPTIONS] [--source SOURCE_ROOT] [--dest DEST_ROOT]
  Options:
    --ask-source Ask for source (even if source is specified).
    --ask-dest Ask for destination (even if dest is specified).
  --source SOURCE_ROOT Source directory from which all contents will be
uploaded. The root directory itself will not be copied.
  --dest DEST_ROOT Destination path on the Drive inside of which SOURCE's
content will be put.
"""
if __name__ == '__main__':
    parser = argparse.ArgumentParser(usage=USAGE)
    parser.add_argument('--ask-source', action='store_true', default=False)
    parser.add_argument('--ask-dest', action='store_true', default=False)
    parser.add_argument('--source')
    parser.add_argument('--dest')
    parser.add_argument('--exclude-dir-part')
    parser.add_argument('--max-size')
    parser.add_argument('--skip-confirmation', action='store_true', default=False)
    parser.add_argument('--replace', action='store_true', default=False)
    args = parser.parse_args()

    if args.ask_source or not args.source:
        args.source = ask_for_source(args.source)
    
    if not args.source:
        print('No source selected')
        sys.exit(1)
        
    if args.ask_dest or not args.dest:
        args.dest = ask_for_dest(args.dest)

    if not args.dest:
        print('No destination specified')
        sys.exit(1)
        
    main(args.source, args.dest, { 'max_size': process_human_size(args.max_size), 'skip_confirmation': args.skip_confirmation, 'exclude_dir': args.exclude_dir_part, 'replace' : args.replace })
