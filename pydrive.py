import os
import os.path
import sys
import time
import argparse
import inspect
import mimetypes
import signal
import tkinter as tk
from tkinter import filedialog
from tkinter import simpledialog
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

#===============================================================================
# Constants

# Configurable
MAX_UPLOAD_SIZE = 100 * 1024 * 1024 # 100 MB
LAST_EXECUTION_LOG = 'lastrun.log'
DONT_UPLOAD_EXTENSIONS = [
    '.db',
]

# Not configurable
AUTH_SCOPES = [ 'https://www.googleapis.com/auth/drive' ]
FOLDER_TYPE_FILTER = "mimeType='application/vnd.google-apps.folder'"
NOT_FOLDER_TYPE_FILTER = "mimeType!='application/vnd.google-apps.folder'"
GIGA = 1 * 1024 * 1024 * 1024

# Debug only 0/1
DEBUG_SKIP_CONFIRMATION = 0
DEBUG_TRACE = 0

#===============================================================================
# Auxiliar

if DEBUG_TRACE:
    def debug_trace(*args):
        print('['+inspect.stack()[1].frame.f_code.co_name+']: ', end='')
        print(*args)
else:
    def debug_trace(*args):
        pass

f = open(LAST_EXECUTION_LOG, 'wt')
def print2(*args, **kwargs):
    __builtins__.print(*args, **kwargs)
    endl = '\n' if not 'end' in kwargs else kwargs['end']
    f.write('\t'.join(args))
    f.write(endl)
print = print2

def safe_get_field(struct, *args):
    for field in args:
        try:
            struct = struct[field]
        except (IndexError, KeyError, TypeError):
            return None
    return struct

def dbg_print_return(value):
    print(str(value))
    return value

def dbg_print_list(lst):
    try:
        for i in range(len(lst)):
            print(i, str(lst[i]))
    except:
        pass

def clean_path(path):
    path = path.replace('\\', '/')
    while path.endswith('/'):
        path = path[:-1]
    return path

def make_relative_path(path, root, strict=True):
    path = clean_path(path)
    root = clean_path(root)
    if not path.startswith(root):
        return None if strict else path
    return path[(len(root)+1):]

def result_list_to_map(result_list):
    map = {}
    for result in result_list:
        map[result['name']] = True
    return map

def extract_file_name(full_path):
    return os.path.basename(full_path)

def format_pretty_size(size, decimais=1):
    UNITS = [ 'B', 'KB', 'MB', 'GB', 'TB' ]
    unit = 0
    while size >= 1024:
        size /= 1024
        unit += 1
        if unit == (len(UNITS) - 1):
            break
    return ('%0.'+str(decimais)+'f %s') % (size, UNITS[unit])

def format_pretty_time(seconds):
    hours, rem = divmod(seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return '%02d:%02d:%02d' % (hours, minutes, seconds)

#===============================================================================
# Google Drive

"""
Authenticate me via OAuth.
"""
def oauth_me(secret_file):
    debug_trace(secret_file)
    AUTH_TOKEN_FILE = 'token.json'
    creds = None
    if os.path.exists(AUTH_TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(AUTH_TOKEN_FILE, AUTH_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                secret_file, AUTH_SCOPES)
            creds = flow.run_local_server(port=0)
        with open(AUTH_TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    return creds

"""
Build a query for the 'q' parameter from multiple subfilters. Always use this.
"""
def _build_query(*args):
    return ' and '.join(["trashed=false", *args])

"""
Make a 'name' filter.
"""
def _name_filter(name):
    return "name='%s'" % name

"""
Make a 'parent' filter.
"""
def _parent_filter(parent_id):
    return "'%s' in parents" % parent_id

"""
Execute a file list request and get all pages of it.
"""
def _files_list_all_pages(service, **kwargs):
    fields = kwargs['fields']
    if fields.find('nextPageToken') == -1 and fields != '*':
        kwargs['fields'] = 'nextPageToken, ' + fields
    all_files = []
    pageToken = None
    while True:
        results = service.files().list(**kwargs, pageToken=pageToken).execute()
        all_files.extend(safe_get_field(results, 'files'))
        pageToken = safe_get_field(results, 'nextPageToken')
        if not pageToken:
            return all_files

"""
List all files of a directory.
Returns list of dicts with 'id' and 'name'.
"""
def list_files(service, root_id):
    debug_trace(root_id)
    results = _files_list_all_pages(service,
        q=_build_query(NOT_FOLDER_TYPE_FILTER, _parent_filter(root_id)),
        fields='files(id, name)',
        pageSize=100, orderBy='folder,name')
    return results

"""
List all subdirectories of a directory.
Returns list of dicts with 'id' and 'name'.
"""
def list_subdirs(service, root_id):
    debug_trace(root_id)
    results = _files_list_all_pages(service,
        q=_build_query(FOLDER_TYPE_FILTER, _parent_filter(root_id)),
        fields='files(id, name)',
        pageSize=100, orderBy='name')
    return results

"""
Get the id of an immediate subdirectory (or None if it doesn't exist).
"""
def get_subdir(service, root_id, name):
    debug_trace(root_id, name)
    result = service.files().list(
        q=_build_query(FOLDER_TYPE_FILTER, _parent_filter(root_id), _name_filter(name)),
        fields="files(id)").execute()
    return safe_get_field(result, 'files', 0, 'id')

"""
Make a directory. Flag check_exists prevents directory duplication (yes, it
duplicates), but also adds overhead.
Returns the directory id.
"""
def mkdir(service, root_id, name, check_exists=True):
    debug_trace(root_id, name, check_exists)
    if check_exists:
        existing_id = get_subdir(service, root_id, name)
        if existing_id:
            return existing_id
    result = service.files().create(fields='id', body={
        'name': name, 'parents': [ root_id ],
        'mimeType': 'application/vnd.google-apps.folder'}).execute()
    return result['id']

"""
Get the id of a nested subdirectory (or None if it doesn't exist).
"""
def get_subpath(service, root_id, path, create=False):
    debug_trace(root_id, path, create)
    current_root = root_id
    for dir in filter(None, path.split('/')):
        sub = get_subdir(service, current_root, dir)
        if not sub:
            if not create:
                return None
            else:
                sub = mkdir(service, current_root, dir, check_exists=False)
        current_root = sub
    return current_root

"""
Get the id of a subdirectory by full path from Drive root (or None if it doesn't
exist).
"""
def get_path(service, path, create=False):
    debug_trace(path, create)
    return get_subpath(service, 'root', path, create)

"""
Make sure a full path exists by creating all directories in it as needed.
Returns the last directory's id.
"""
def ensure_path(service, path):
    debug_trace(path)
    return get_path(service, path, create=True)
    
"""
Upload a file to a given directory (by id). Flag check_exists prevents file
duplication (yes, it duplicates), but also adds overhead.
Returns the file id.
"""
def upload_file(service, root_id, full_file_path, progress_callback=None, check_exists=True):
    debug_trace(root_id, full_file_path, check_exists)
    file_name = extract_file_name(full_file_path)
    mimetype = mimetypes.guess_type(file_name)[0] or 'application/octet-stream'
    
    if check_exists:
        existing_id = get_subdir(service, root_id, file_name)
        if existing_id:
            return existing_id

    media = MediaFileUpload(full_file_path,
        mimetype=mimetype,
        chunksize=256*1024,
        resumable=True)
    request = service.files().create(fields='id', body={
        'name': file_name, 'parents': [ root_id ],
        'mimeType': mimetype},
        media_body=media)
    media.stream()
    response = None
    if progress_callback:
        progress_callback(0, 0) # shows empty at first
    while response is None:
        status, response = request.next_chunk()
        if status and progress_callback:
            progress_callback(status.resumable_progress, status.total_size)
    return response['id']

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
    num_progress_full = int(current * width / total)
    num_progress_empty = width - num_progress_full
    return ('#' * num_progress_full) + ('-' * num_progress_empty)

"""
Helper for formatting the progress bar.
"""
def _format_progress_percent(current, total):
    if total == 0:
        return '  0%'
    return '% 3d%%' % round(current * 100 / total)

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
def main(source_root, dest_root):
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
    service = build('drive', 'v3', credentials=oauth_me(secret_file))
    print('CONNECTED')
    
    print('Searching for the destination path in your Drive... ', end='')
    dest_root_id = get_path(service, dest_root)
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
    if not DEBUG_SKIP_CONFIRMATION and input('Are you sure [y/N]? ').upper() != 'Y':
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
        current_dest_id = ensure_path(service, dest_path)
        clear_progress_bar()
        print('Listing files for "%s"...' % dest_path)
        existing_files_map = result_list_to_map(list_files(service, current_dest_id))
        
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
                elif file_size > MAX_UPLOAD_SIZE:
                    print('File "%s" not uploaded due to size (%s)' % (short_file_name,
                        format_pretty_size(file_size)))
                    num_skipped_files += 1
                else:
                    print('Uploading file "%s/%s" (%s)' % (dest_path, file, format_pretty_size(file_size)))
                    #update_progress_bar(num_processed_files, num_source_files)
                    try:
                        callback = lambda progress, total: update_progress_bar(
                            num_processed_files, num_source_files, progress, total)
                            
                        upload_file(service, current_dest_id, full_file_path,
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
python pydrive.py [OPTIONS] [--source SOURCE_ROOT] [--dest DEST_ROOT]
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
        
    main(args.source, args.dest)

"""
Notes:
SSC venv\Lib\site-packages\httplib2\__init__.py:1067&1212 => disable_ssl_certificate_validation=True,
"""