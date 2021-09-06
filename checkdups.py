"""
Raphael Pithan
2021
"""

import os
import sys
import argparse
import tkinter as tk
from tkinter import simpledialog
from datetime import datetime

from auxiliar import *
from drive import *

f = open('dups.log', 'wt')
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

"""
Check result list for duplicates.
"""
def check_dup(result_list, callback):
    last_name = safe_get_field(result_list, 0, 'name') or '<empty>'
    for i in range(1, len(result_list)):
        curr_name = safe_get_field(result_list, i, 'name') or '<empty>'
        if curr_name == last_name:
            callback(curr_name)
        last_name = curr_name

"""
Check directory recursively.
"""
def check_dir(drive, id, path):
    print('Checking directory ' + path)
    dirs = drive.list_subdirs(id)
    files = drive.list_files(id)
    check_dup(dirs, lambda n: print('** Duplicate directory found: ' + n))
    check_dup(files, lambda n: print('** Duplicate files found: ' + n))
    for dir in dirs:
        id = safe_get_field(dir, 'id')
        name = safe_get_field(dir, 'name') or '<empty>'
        if id:            
            check_dir(drive, id, path + '/' + name)
        else:
            print('** Empty directory id found in ' + path)

"""
Main. See script's doc bellow for more information.
"""
def main(drive_root):
    print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

    secret_file = get_client_secret_file()
    if not secret_file:
        print('No client secret file found!')
        sys.exit(1)
    print('Connecting to Google Drive... ', end='')
    drive = Drive()
    drive.connect(secret_file)
    print('CONNECTED')

    check_dir(drive, drive.get_path(drive_root), drive_root)

USAGE = """
python getdups.py [OPTIONS] [--dest drive_root]
  Options:
    --ask-dest Ask for destination (even if dest is specified).
  --dest drive_root Destination path on the Drive which will be checked for
duplicates.
"""
if __name__ == '__main__':
    parser = argparse.ArgumentParser(usage=USAGE)
    parser.add_argument('--ask-dest', action='store_true', default=False)
    parser.add_argument('--dest')
    args = parser.parse_args()

    if args.ask_dest or not args.dest:
        args.dest = ask_for_dest(args.dest)

    if not args.dest:
        print('No destination specified')
        sys.exit(1)
        
    main(args.dest)
