import os
import sys
import argparse
import dropbox

DEFAULT_TIMEOUT = 60 # how many seconds until we abandon trying to connect with Dropbox
DEFAULT_CHUNK_SIZE = 150*1024*1024 # dropbox says ideally <150MB per chunk
UPLOAD = 'upload'
DOWNLOAD = 'download'
ZIP = '.zip'

def send_to_dropbox(local_filepath, dropbox_directory, client, root=None):
    '''
    Sends a single file to Dropbox.  See behavior below.

    If you pass local_filepath (e.g. /home/user/foo.txt) with root=None (default) 
    then it will end up in <dropbox_directory>/foo.txt

    For the case where the caller is trying to recursively upload a directory,
    the caller should pass an argument for root.  For instance, given the following
    directory:
    .
    ├── foo.txt
    ├── dir1
    └── dir2
        ├── bar.txt
        └── dir3
            └── baz.txt
    If are uploading ./dir2/dir3/baz.txt, the root of this tree is at /home/user/somedir, 
    the caller should pass local_filepath=/home/user/somedir/dir2/dir3/baz.txt and
    root="/home/user/somedir" and the files will be upload to:
    <dropbox_directory>/dir2/dir3/baz.txt

    The name of the file is unchanged
    (e.g. /home/user/abc.txt will end up as <dropbox_directory>/abc.txt)

    `local_filepath` is the path on the VM/container.  Should be absolute.
    `dropbox_directory` is the folder where the files are deposited (in Dropbox)
    `client` is an instance of dropbox.Dropbox, which has already done the token exchange.
    '''

    file_size = os.path.getsize(local_filepath)
    stream = open(local_filepath, 'rb')
    
    # setup the paths in dropbox
    if root is None:
        relpath = os.path.basename(local_filepath)
    else:
        relpath = os.path.relpath(local_filepath, root)
    path_in_dropbox = '%s/%s' % (dropbox_directory, relpath)

    # if the file is smaller than the chunk size, just do a simple upload
    if file_size <= DEFAULT_CHUNK_SIZE:
        print('Performing upload in a single chunk.')
        client.files_upload(stream.read(), path_in_dropbox)
        print('Completed upload for %s' % local_filepath)
    # if the file is larger, we have to send it in multiple chunks
    else:
        i = 1
        session_start_result = client.files_upload_session_start(stream.read(DEFAULT_CHUNK_SIZE))
        cursor=dropbox.files.UploadSessionCursor(session_start_result.session_id, offset=stream.tell())
        commit=dropbox.files.CommitInfo(path=path_in_dropbox)
        while stream.tell() < file_size:
            print('Sending chunk %s' % i)
            try:
                if (file_size-stream.tell()) <= DEFAULT_CHUNK_SIZE:
                    print('Finishing transfer and committing')
                    client.files_upload_session_finish(stream.read(DEFAULT_CHUNK_SIZE), cursor, commit)
                else:
                    print('About to send chunk')
                    print('Prior to chunk transfer, cursor=%d, stream=%d' % (cursor.offset, stream.tell()))
                    client.files_upload_session_append_v2(stream.read(DEFAULT_CHUNK_SIZE), cursor)
                    cursor.offset = stream.tell()
                    print('Done with sending chunk %s' % i)
            except dropbox.exceptions.ApiError as ex:
                print('ERROR: Raised ApiError!')
                if ex.error.is_incorrect_offset():
                    print('ERROR: The error raised was an offset error.  Correcting the cursor and stream offset')
                    correct_offset = ex.error.get_incorrect_offset().correct_offset
                    cursor.offset = correct_offset
                    stream.seek(correct_offset)
                else:
                    print('ERROR: API error was raised, but was not offset error')
                    raise ex

            except requests.exceptions.ConnectionError as ex:
                print('ERROR: Caught a ConnectionError exception')
                # need to rewind the stream
                print('At this point, cursor=%d, stream=%d' % (cursor.offset, stream.tell()))
                cursor_offset = cursor.offset
                stream.seek(cursor_offset)
                print('After rewind, cursor=%d, stream=%d' % (cursor.offset, stream.tell()))
                print('Go try that chunk again')
            except requests.exceptions.RequestException as ex:
                print('ERROR: Caught an exception during chunk transfer')
                print('ERROR: Following FAILED chunk transfer, cursor=%d, stream=%d' % (cursor.offset, stream.tell()))
                raise ex
            i += 1
    stream.close()


def pull_folder_from_dropbox(dropbox_path, local_path, client):
    '''
    Downloads a folder as a ZIP archive

    Note that the SDK function we use requires that the folder is <20G
    and have fewer than 10,000 total files.  Any single file must be less than 4G.

    `dropbox_path` is the resource we are trying to download
    `local_path` is where the ZIP will be downloaded locally (e.g. /home/user/foo.zip)
    `client` is an instance of dropbox.Dropbox, the authenticated Dropbox API client
    '''

    try:

        print('Starting folder download...')

        client.files_download_zip_to_file(local_path, dropbox_path)

        print('Completed download.  Zip file available at %s' % local_path)

    except dropbox.exceptions.ApiError as ex:
        err = ex.error
        if err.too_large:
            print('The folder was too large to download as a ZIP.  Resorting to individual downloads...')
            fallback_to_individual_downoads(dropbox_path, local_path, client)
        else:
            print('There was an error downloading the folder from Dropbox (%s) to local path: %s' % (dropbox_path, local_path))
            print(ex)
            sys.exit(1)


def fallback_to_individual_downoads(dropbox_path, local_path, client):
    '''
    This is used if the folder is too large and we need to resort to individually downloading the files
    '''
    all_files = client.files_list_folder(dropbox_path)
    # we initially had requested a zip archive be downloaded.
    # Now, we assume they want the files in the same directory
    local_dir = os.path.dirname(local_path)
    for f in all_files.entries:
        p = f.path_lower
        local_path = os.path.join(local_dir, os.path.basename(p))
        pull_file_from_dropbox(p, local_path, client)


def pull_file_from_dropbox(dropbox_path, local_path, client):
    '''
    Downloasd a file from Dropbox.

    `dropbox_path` is the resource we are trying to download
    `local_path` is where the ZIP will be downloaded locally (e.g. /home/user/foo.zip)
    `client` is an instance of dropbox.Dropbox, the authenticated Dropbox API client
    '''
    try:
        print('Starting file download...')
        client.files_download_to_file(local_path, dropbox_path)
        print('Completed file download.  File is available at %s' % local_path)
    except dropbox.exceptions.ApiError as ex:
        print('There was an error downloading from Dropbox (%s) to local path: %s' % (dropbox_path, local_path))
        print(ex)
        sys.exit(1)


def parse_args():
    main_parser = argparse.ArgumentParser(prog='Dropbox upload/downloader')
    subparsers = main_parser.add_subparsers(help='Subcommand', dest='subcommand')

    uploader_parser = subparsers.add_parser(UPLOAD, 
        help='Upload one or more folders or files to Dropbox',
        description = '''
        Given a list of paths, this program will upload them.
        If the path arguments contain directories, this will recursively upload those.
        '''
    )
    downloader_parser = subparsers.add_parser(DOWNLOAD, 
        help='Download from Dropbox', 
        description='''Downloads a file or folder.  
        If a folder, it must total less than 20GB and contain
        fewer than 10,000 files.  All individual files must be <4GB.
        The result will be a zip archive if a folder is requested.
        If a single file, the Dropbox API does not mention any restrictions on size. 
        '''
    )

    main_parser.add_argument("-t", "--token", help="The access token for Dropbox", dest='access_token', required=True)

    # for uploads, we allow either folders or files in the list of things to upload.  Folders will be recursively transferred.
    uploader_parser.add_argument("path", help="The paths of the files or folders to transfer", nargs='+')
    uploader_parser.add_argument("-d", help='The "root" folder in Dropbox where the files/folders will go', dest='dropbox_destination_root', required=True)

    # for downloads, we either pull entire directories or individual files
    downloader_parser.add_argument("-o", help='''The local path for the file or folder.  
        If you are downloading a folder it will create
        a ZIP archive, so we require that the file extension be "zip".  If a file, it does not matter.''', 
        dest='resource_path', required=True)
    group = downloader_parser.add_mutually_exclusive_group(required=True) 
    group.add_argument("-d", help="The folder in Dropbox", dest='dropbox_folder')
    group.add_argument("-f", help="The path of the file in Dropbox", dest='dropbox_file')

    params = {}
    args = main_parser.parse_args()
    params['token'] = args.access_token 
    params['subcommand'] = args.subcommand

    if params['subcommand'] == UPLOAD:
        params['dropbox_destination_root'] = args.dropbox_destination_root
        params['paths'] = args.path
    elif params['subcommand'] == DOWNLOAD:
        params['resource_path'] = args.resource_path
        if args.dropbox_file:
            params['is_folder'] = False
            params['dropbox_source'] = args.dropbox_file
        else:
            params['is_folder'] = True
            params['dropbox_source'] = args.dropbox_folder
    else:
        sys.exit(1)

    return params



if __name__ == '__main__':
    try:
        # get the arguments passed to this script
        params = parse_args()
        token = params['token']

        client = dropbox.dropbox.Dropbox(token, timeout=DEFAULT_TIMEOUT)

        if params['subcommand'] == UPLOAD:

            local_filepaths = params['paths']
            dropbox_directory = params['dropbox_destination_root']
            skipped_paths = []

            for local_filepath in local_filepaths:
                local_filepath = os.path.abspath(local_filepath)
                if os.path.exists(local_filepath):
                    if os.path.isfile(local_filepath):
                        send_to_dropbox(local_filepath, 
                            dropbox_directory, 
                            client
                        )
                    elif os.path.isdir(local_filepath):
                        # handle a directory.  Will not do anything with empty dirs
                        for root, dirs, files in os.walk(local_filepath):
                            for f in files:
                                full_path = os.path.join(root, f)
                                send_to_dropbox(full_path, 
                                    dropbox_directory, 
                                    client,
                                    root=os.path.dirname(local_filepath)
                                )
                    else:
                        print('WARN: the following path was not determined to be a file or a directory: %s' % local_filepath)
                        skipped_paths.append(local_filepath)
                else: # a path did not exist
                    skipped_paths.append(local_filepath)

            if len(skipped_paths) > 0:
                print('WARN: The following were skipped- please check the paths exist.')
                print('\n'.join(skipped_paths))

        else: # download
            local_path = params['resource_path']
            if params['is_folder']:
                if not local_path.endswith(ZIP):
                    local_path = local_path + ZIP
                    print('WARN: The download path did not have the proper zip extension.  Adding it.  The zip archive will be available at %s' % local_path)
                pull_folder_from_dropbox(params['dropbox_source'], local_path, client)
            else: # regular file
                pull_file_from_dropbox(params['dropbox_source'], local_path, client)

    except Exception as ex:
        print('ERROR: Caught some unexpected exception.')
        print(str(type(ex)))
        print(str(ex))
