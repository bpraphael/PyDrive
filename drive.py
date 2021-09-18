"""
Raphael Pithan
2021
"""

import os.path
import mimetypes
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials

from auxiliar import *

AUTH_SCOPES = [ 'https://www.googleapis.com/auth/drive' ]
FOLDER_TYPE_FILTER = "mimeType='application/vnd.google-apps.folder'"
NOT_FOLDER_TYPE_FILTER = "mimeType!='application/vnd.google-apps.folder'"

"""
Class for accessing Google Drive files.
"""
class Drive:
    """
    Constructor.
    """
    def __init__(self):
        self.service = None
        
    """
    Authenticate me via OAuth.
    """
    def _oauth_me(self, secret_file):
        debug_trace(secret_file)
        AUTH_TOKEN_FILE = 'token.json'
        creds = None
        if os.path.exists(AUTH_TOKEN_FILE):
            creds = Credentials.from_authorized_user_file(AUTH_TOKEN_FILE,
                AUTH_SCOPES)
        if not creds or not creds.valid:
            refreshed = False
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    refreshed = True
                except RefreshError:
                    os.remove(AUTH_TOKEN_FILE)
            if not refreshed:
                flow = InstalledAppFlow.from_client_secrets_file(
                    secret_file, AUTH_SCOPES)
                creds = flow.run_local_server(port=0)
            with open(AUTH_TOKEN_FILE, 'w') as token:
                token.write(creds.to_json())
        return creds

    """
    Build a query for the 'q' parameter from multiple subfilters. Always use this.
    """
    def _build_query(self, *args):
        return ' and '.join(["trashed=false", *args])

    """
    Make a 'name' filter.
    """
    def _name_filter(self, name):
        return "name='%s'" % name

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
        self.service = build('drive', 'v3',
            credentials=self._oauth_me(secret_file))
        return self.service is not None

    """
    List all files of a directory.
    Returns list of dicts with 'id' and 'name'.
    """
    def list_files(self, root_id):
        debug_trace(root_id)
        results = self._files_list_all_pages(
            q=self._build_query(NOT_FOLDER_TYPE_FILTER, self._parent_filter(root_id)),
            fields='files(id, name)',
            pageSize=100, orderBy='name')
        return results

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
    Upload a file to a given directory (by id). Flag check_exists prevents file
    duplication (yes, it duplicates), but also adds overhead.
    Returns the file id.
    """
    def upload_file(self, root_id, full_file_path, progress_callback=None, check_exists=True):
        debug_trace(root_id, full_file_path, check_exists)
        file_name = extract_file_name(full_file_path)
        mimetype = mimetypes.guess_type(file_name)[0] or 'application/octet-stream'
        
        if check_exists:
            existing_id = self.get_subdir(root_id, file_name)
            if existing_id:
                return existing_id

        media = MediaFileUpload(full_file_path,
            mimetype=mimetype,
            chunksize=256*1024,
            resumable=True)
        request = self.service.files().create(fields='id', body={
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
