"""
Raphael Pithan
2021
"""

import os.path
import inspect

DEBUG_TRACE = 0

# Debug -----------

if DEBUG_TRACE:
    def debug_trace(*args):
        print('['+inspect.stack()[1].frame.f_code.co_name+']: ', end='')
        print(*args)
else:
    def debug_trace(*args):
        pass

def debug_print_return(value):
    print(str(value))
    return value

def debug_print_list(lst):
    try:
        for i in range(len(lst)):
            print(i, str(lst[i]))
    except:
        pass

# Helper ----------

def safe_get_field(struct, *args):
    for field in args:
        try:
            struct = struct[field]
        except (IndexError, KeyError, TypeError):
            return None
    return struct

# Path ------------

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

def extract_file_name(full_path):
    return os.path.basename(full_path)

# Format ----------

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

# Other -----------

def result_list_to_map(result_list):
    map = {}
    for result in result_list:
        map[result['name']] = result['id'] if 'id' in result else True
    return map
