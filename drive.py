"""
Raphael Pithan
2021
"""

import sys
import os.path
import mimetypes
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials

from auxiliar import *

AUTH_SCOPES = [ 'https://www.googleapis.com/auth/drive' ]
AUTH_SCOPES_READ_ONLY = [ 'https://www.googleapis.com/auth/drive.readonly' ]
AUTH_SCOPE_ACTIVITY = 'https://www.googleapis.com/auth/drive.activity.readonly'
FOLDER_TYPE_FILTER = "mimeType='application/vnd.google-apps.folder'"
NOT_FOLDER_TYPE_FILTER = "mimeType!='application/vnd.google-apps.folder'"

"""
Print error message as in print.
"""
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

"""
Class for accessing Google Drive files.
"""
class Drive:
    """
    Constructor.
    """
    def __init__(self, read_only=False, token_file=None, include_activity=False):
        self.service = None
        self.credentials = None
        self.read_only = read_only
        self.token_file = token_file or 'token.json'
        self.include_activity_api = include_activity
        self.activity_service = None
    
    """
    Authenticate me via OAuth.
    """
    def _oauth_me(self, secret_file):
        debug_trace(secret_file)
        creds = None
        requested_auth_scopes = AUTH_SCOPES if not self.read_only else AUTH_SCOPES_READ_ONLY
        if self.include_activity_api:
            requested_auth_scopes = requested_auth_scopes.copy()
            requested_auth_scopes.append(AUTH_SCOPE_ACTIVITY)
        if os.path.exists(self.token_file):
            creds = Credentials.from_authorized_user_file(self.token_file,
                requested_auth_scopes)
        if not creds or not creds.valid:
            refreshed = False
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    refreshed = True
                except RefreshError:
                    os.remove(self.token_file)
            if not refreshed:
                flow = InstalledAppFlow.from_client_secrets_file(
                    secret_file, requested_auth_scopes)
                creds = flow.run_local_server(port=0)
            with open(self.token_file, 'w') as token:
                token.write(creds.to_json())
        return creds

    """
    Build a query for the 'q' parameter from multiple subfilters. Always use this.
    """
    def _build_query(self, *args):
        return ' and '.join([q for q in ["trashed=false", *args] if q is not None and len(q) > 0])

    """
    Make a 'name' filter.
    """
    def _name_filter(self, name, exact=True):
        name = name.replace('\\', '\\\\').replace("'", "\\'")
        if exact:
            return "name='%s'" % name
        else:
            return "name contains '%s'" % name

    """
    Make a 'parent' filter.
    """
    def _parent_filter(self, parent_id):
        return "'%s' in parents" % parent_id

    """
    Execute a file list request and get all pages of it.
    """
    def _files_list_all_pages(self, **kwargs):
        fields = kwargs['fields']
        if fields.find('nextPageToken') == -1 and fields != '*':
            kwargs['fields'] = 'nextPageToken, ' + fields
        all_files = []
        pageToken = None
        while True:
            results = self.service.files().list(**kwargs, pageToken=pageToken).execute()
            all_files.extend(safe_get_field(results, 'files'))
            pageToken = safe_get_field(results, 'nextPageToken')
            if not pageToken:
                return all_files
                
    """
    Connect to the service.
    """
    def connect(self, secret_file):
        self.credentials = self._oauth_me(secret_file)
        self.service = build('drive', 'v3', credentials=self.credentials)
        return self.service is not None
    
    """
    Connect to the activity service (v2 API).
    """
    def connect_activity(self):
        if self.activity_service is None and self.include_activity_api:
            self.activity_service = build('driveactivity', 'v2', credentials=self.credentials)
        return self.activity_service is not None
    
    """
    Duplicate this service instance with a new http backend (make thread-safe).
    """
    def duplicate_service(self):
        new_service = Drive()
        new_service.credentials = self.credentials
        new_service.service = build('drive', 'v3', credentials=new_service.credentials)
        return new_service

    """
    List all files of a directory.
    Returns list of dicts with 'id' and 'name'.
    """
    def list_files(self, root_id, query=None, fields='id, name', order='name'):
        debug_trace(root_id)
        results = self._files_list_all_pages(
            q=self._build_query(NOT_FOLDER_TYPE_FILTER, self._parent_filter(root_id), query),
            fields='files('+fields+')',
            pageSize=100, orderBy=order)
        return results
    
    """
    Get the ids of the (possibly) multiple files with the given name (or None if
    it doesn't exist).
    """
    def get_files(self, root_id, name):
        debug_trace(root_id, name)
        result = self.service.files().list(
            q=self._build_query(NOT_FOLDER_TYPE_FILTER, self._parent_filter(root_id),
                self._name_filter(name)),
            fields="files(id)").execute()
        res = safe_get_field(result, 'files')
        return res if len(res) > 0 else None

    """
    Get the ids of the (possibly) multiple parents of the given id.
    """
    def get_parents(self, id):
        debug_trace(id)
        result = self.service.files().get(fileId=id,
            fields="parents").execute()
        res = safe_get_field(result, 'parents')
        return res if res != None else []

    """
    List all subdirectories of a directory.
    Returns list of dicts with 'id' and 'name'.
    """
    def list_subdirs(self, root_id):
        debug_trace(root_id)
        results = self._files_list_all_pages(
            q=self._build_query(FOLDER_TYPE_FILTER, self._parent_filter(root_id)),
            fields='files(id, name)',
            pageSize=100, orderBy='name')
        return results

    """
    List ALL directories (whole drive) based on a word in it's name.
    Returns list of dicts with 'id', 'name' and 'parents'.
    """
    def list_dirs_query(self, name):
        debug_trace(name)
        results = self._files_list_all_pages(
            q=self._build_query(FOLDER_TYPE_FILTER, self._name_filter(name, exact=False)),
            fields='files(id, name, parents)',
            pageSize=100, orderBy='name')
        return results

    """
    Get the id of an immediate subdirectory (or None if it doesn't exist).
    """
    def get_subdir(self, root_id, name):
        debug_trace(root_id, name)
        result = self.service.files().list(
            q=self._build_query(FOLDER_TYPE_FILTER, self._parent_filter(root_id),
                self._name_filter(name)),
            fields="files(id)").execute()
        return safe_get_field(result, 'files', 0, 'id')

    """
    Make a directory. Flag check_exists prevents directory duplication (yes, it
    duplicates), but also adds overhead.
    Returns the directory id.
    """
    def mkdir(self, root_id, name, check_exists=True):
        debug_trace(root_id, name, check_exists)
        if check_exists:
            existing_id = self.get_subdir(root_id, name)
            if existing_id:
                return existing_id
        result = self.service.files().create(fields='id', body={
            'name': name, 'parents': [ root_id ],
            'mimeType': 'application/vnd.google-apps.folder'}).execute()
        return result['id']

    """
    Get the id of a nested subdirectory (or None if it doesn't exist).
    """
    def get_subpath(self, root_id, path, create=False):
        debug_trace(root_id, path, create)
        current_root = root_id
        for dir in filter(None, path.split('/')):
            sub = self.get_subdir(current_root, dir)
            if not sub:
                if not create:
                    return None
                else:
                    sub = self.mkdir(current_root, dir, check_exists=False)
            current_root = sub
        return current_root

    """
    Get the id of a subdirectory by full path from Drive root (or None if it doesn't
    exist).
    """
    def get_path(self, path, create=False):
        debug_trace(path, create)
        return self.get_subpath('root', path, create)

    """
    Make sure a full path exists by creating all directories in it as needed.
    Returns the last directory's id.
    """
    def ensure_path(self, path):
        debug_trace(path)
        return self.get_path(path, create=True)
    
    """
    Download a file (by id).
    Returns the file id.
    """
    def download_file(self, file_id, output_file=None, progress_callback=None):
        debug_trace(file_id)
        request = self.service.files().get_media(fileId=file_id)
        file = io.BytesIO() if output_file == None else output_file
        media = MediaIoBaseDownload(file, request, chunksize=1024*1024)
        if progress_callback:
            progress_callback(0, 0) # shows empty at first
        done = False
        while done is False:
            status, done = media.next_chunk()
            if status and progress_callback:
                progress_callback(status.resumable_progress, status.total_size)
        file.seek(0)
        return file
        
    """
    Upload a file to a given directory (by id). Flag check_exists prevents file
    duplication (yes, it duplicates), but also adds overhead.
    Returns the file id.
    """
    def upload_file(self, root_id, full_file_path, progress_callback=None, check_exists=True, replace=False):
        debug_trace(root_id, full_file_path, check_exists, replace)
        file_name = extract_file_name(full_file_path)
        mimetype = mimetypes.guess_type(file_name)[0] or 'application/octet-stream'
        
        if check_exists or replace:
            existing_files = self.get_files(root_id, file_name)
            if existing_files:
                if not replace:
                    return safe_get_field(existing_files, 0, 'id')
                else:
                    for file in existing_files:
                        eprint('INFO: deleting a file, ' + str(file['id']) + ' ' + file_name)
                        self.service.files().delete(fileId=file['id']).execute()
        
        media = MediaFileUpload(full_file_path,
            mimetype=mimetype,
            chunksize=1024*1024,
            resumable=True)
        request = self.service.files().create(fields='id', body={
            'name': file_name, 'parents': [ root_id ],
            'mimeType': mimetype},
            media_body=media)
        response = None
        if progress_callback:
            progress_callback(0, 0) # shows empty at first
        while response is None:
            status, response = request.next_chunk()
            if status and progress_callback:
                progress_callback(status.resumable_progress, status.total_size)
        return response['id']

    """
    Get the last modified time for a file or directory. Based on the activity
    API, returns the correct time for folders which had modifications deep
    within it's subtrees.
    """
    def get_mtime(self, id):
        debug_trace(id)
        self.connect_activity()
        result = self.activity_service.activity().query(body={
            'ancestorName': 'items/' + str(id), 'pageSize': 1}).execute()
        return safe_get_field(result, 'activities', 0, 'timestamp')
