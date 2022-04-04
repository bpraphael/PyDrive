"""
Program for uploading files to Google Drive.
Run with --help for more info.

Raphael Pithan
2021
"""

import os
import os.path
import sys
import time
import argparse
import signal
import tkinter as tk
from tkinter import filedialog
from tkinter import simpledialog
from datetime import datetime

from auxiliar import *
from drive import *

#===============================================================================
# Constants

# Configurable ----

MAX_UPLOAD_SIZE = 100 * 1024 * 1024 # 100 MB
LAST_EXECUTION_LOG = 'run%s.log'
DONT_UPLOAD_EXTENSIONS = [
    '.db', '.py', '.bat'
]

# Not configurable

GIGA = 1 * 1024 * 1024 * 1024

# Debug -----------

DEBUG_SKIP_CONFIRMATION = 0

# Log -------------

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

"""
Helper for formatting the progress bar.
"""
def _format_progress_bar(width, current, total):
    if total == 0:
        return '-' * width
    current = min(max(0, current), total)
    num_progress_full = int(current * width / total)
    num_progress_empty = width - num_progress_full
    return ('#' * num_progress_full) + ('-' * num_progress_empty)

"""
Helper for formatting the progress bar.
"""
def _format_progress_percent(current, total):
    if total == 0:
        return '  0%'
    current = min(max(0, current), total)
    return ('%d%%' % round(current * 100 / total)).rjust(4)

"""
Show/update the progress bar.
"""
def update_progress_bar(files_current, files_total, upload_current=0, upload_total=0):
    BAR_WIDTH = 30
    files_bar = _format_progress_bar(BAR_WIDTH, files_current, files_total)
    upload_bar = _format_progress_bar(BAR_WIDTH, upload_current, upload_total)
    files_percent = _format_progress_percent(files_current, files_total)
    upload_percent = _format_progress_percent(upload_current, upload_total)
    
    sys.stdout.write('%s |%s|%s| %s (%d/%d)\r' % (
        upload_percent, upload_bar, files_bar,
        files_percent, files_current, files_total))
    sys.stdout.flush()

"""
Clear the progress bar before printing another message.
"""
def clear_progress_bar():
    sys.stdout.write((' ' * 85) + '\r') # clear the progress bar
    sys.stdout.flush()

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
        
#===============================================================================
# Main

# Global
g_stop_loop = False

"""
Handle Ctrl+C during the main loop.
"""
def signal_handler(sig, frame):
    global g_stop_loop
    g_stop_loop = True
    print('Ctrl+C')

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
    print('Operation can be interrupted at any time by >>>Ctrl+C<<<.')
    if not DEBUG_SKIP_CONFIRMATION and not options['skip_confirmation'] and input('Are you sure [y/N]? ').upper() != 'Y':
        print('Operation aborted!')
        sys.exit(1)
    
    # --- It's show time ---
    print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    
    global g_stop_loop
    signal.signal(signal.SIGINT, signal_handler)
    
    size_uploaded_files = 0
    num_uploaded_files = 0
    num_upload_errors = 0
    num_existing_files = 0
    num_skipped_files = 0
    num_processed_files = 0
    error_streak = 0
    ERROR_STREAK_WAIT = 5
    ERROR_STREAK_ABORT = 10
    start_time = time.time()
    
    # Walk each subdir in source (including the root)
    for path, dirs, files in os.walk(source_root):
        # Calculate the destination path for this directory in the Drive and
        # obtain the list of files that already exist there (as a hash map)
        relative_path = make_relative_path(path, source_root)
        dest_path = clean_path(dest_root + '/' + relative_path)
        current_dest_id = drive.ensure_path(dest_path)
        clear_progress_bar()
        print('Listing files for "%s"...' % dest_path)
        existing_files_map = result_list_to_map(drive.list_files(current_dest_id))
        
        # Walk each file in this subdir
        for file in files:
            num_processed_files += 1
            if not file in existing_files_map:
                # File does not exist in destination, upload it
                short_file_name = relative_path + '/' + file
                full_file_path = clean_path(path + '/' + file)
                file_size = os.path.getsize(full_file_path)
                clear_progress_bar()
                if os.path.splitext(full_file_path)[-1] in DONT_UPLOAD_EXTENSIONS:
                    print('File "%s" not uploaded due to prevented extension' % short_file_name)
                    num_skipped_files += 1
                elif not options['no_max_size'] and file_size > MAX_UPLOAD_SIZE:
                    print('File "%s" not uploaded due to size (%s)' % (short_file_name,
                        format_pretty_size(file_size)))
                    num_skipped_files += 1
                else:
                    print('Uploading file "%s/%s" (%s)' % (dest_path, file, format_pretty_size(file_size)))
                    #update_progress_bar(num_processed_files, num_source_files)
                    try:
                        callback = lambda progress, total: update_progress_bar(
                            num_processed_files, num_source_files, progress, total)
                            
                        drive.upload_file(current_dest_id, full_file_path,
                            progress_callback=callback, check_exists=False)
                            
                        size_uploaded_files += file_size
                        num_uploaded_files += 1
                        error_streak = 0
                    except Exception as e:
                        print('**File upload error: ' + str(e))
                        num_upload_errors += 1
                        error_streak += 1
                        
                    # Error handling
                    if error_streak >= ERROR_STREAK_ABORT:
                        g_stop_loop = True
                    elif error_streak >= ERROR_STREAK_WAIT:
                        print('Too many sequential errors, waiting 30 seconds before continuing...')
                        time.sleep(30)
            else:
                # File already exists in destination
                num_existing_files += 1
            
            if g_stop_loop:
                break # for file
        if g_stop_loop:
            break # for path

    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # Show final statistics
    print('\n--Operation completed--')
    print('Time taken: ' + format_pretty_time(elapsed_time))
    print('Average upload speed:  %s/s | %d file(s)/min' % (
        format_pretty_size(size_uploaded_files / elapsed_time),
        round(num_uploaded_files * 60 / elapsed_time)))
    if size_uploaded_files != 0:
        print('Time to 1 GB: %s' % format_pretty_time(GIGA * elapsed_time / size_uploaded_files))
    print('%d file(s) uploaded (%s)' % (
        num_uploaded_files,
        format_pretty_size(size_uploaded_files)))
    print('%d file(s) failed to upload' % num_upload_errors)
    if num_skipped_files > 0:
        print('%d file(s) skipped' % num_skipped_files)
    print('%d file(s) already existed' % num_existing_files)
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
    parser.add_argument('--no-max-size', action='store_true', default=False)
    parser.add_argument('--skip-confirmation', action='store_true', default=False)
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
        
    main(args.source, args.dest, { 'no_max_size': args.no_max_size, 'skip_confirmation': args.skip_confirmation })
