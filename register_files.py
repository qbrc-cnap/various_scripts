import requests
import sys
import argparse
import os
import subprocess
import json
import datetime


DOMAIN = 'https://cnap.tm4.org'
USERS_ENDPOINT = '/users/'
RESOURCE_ENDPOINT = '/resources/'
TOKEN_ENDPOINT = '/api-token-auth/'
GS_PREFIX = 'gs://'
TOKEN = 'token'
FILES = 'files'
CNAP_USER = 'cnap_user'
EXPIRY = 'expiry'

# the request payload needs to have the following keys:
PAYLOAD_TEMPLATE = {
    "source": "google",
    "source_path": "",
    "path": "",
    "name": "",
    "size": 0,
    "owner": None,
    "is_active": True,
    "originated_from_upload": False,
}


def parse_files(resource_list):
    '''
    Parses the list of files provided.  If they are missing the file prefix, skip and warn
    '''
    final_list = set()
    for f in resource_list:
        if not f.startswith(GS_PREFIX):
            print('WARN: File %s does not have the required format, starting with "%s". Skipping.' % (f, GS_PREFIX))
        else:
            final_list.add(f)
    return list(final_list)


def get_token(username, password):
    '''
    Exchanges username/pwd for an auth token to make subsequent requests to the API
    '''
    url = DOMAIN + TOKEN_ENDPOINT
    payload = {'username': username, 'password': password}
    headers = {'Content-Type': 'application/json'}
    r = requests.post(url, data=json.dumps(payload), headers=headers)
    j = r.json()
    try:
        return j['token']
    except:
        print('Could not get auth token.  Check user/pass or try again.')
        sys.exit(1)


def validate_datestring(date_text):
    '''
    Validates that the date string supplied as an optional commandline arg
    is valid and at least one day in the future.
    '''
    try:
        d = datetime.datetime.strptime(date_text, '%Y-%m-%d')
        now = datetime.datetime.now()
        dt = d - now
        if dt.days < 1:
            print('The date you specified for expiration has already passed.  Please specify a date at least one day from now.  Exiting')
            sys.exit(1)
    except ValueError:
        print('Incorrect data format, should be "YYYY-MM-DD".  Exiting.')
        sys.exit(1)


def parse():
    parser =  argparse.ArgumentParser(description='Register files with the CNAP application.')
    parser.add_argument('-u', '--username', required=True, help='Username of admin user')
    parser.add_argument('-p', '--password', required=True, help='Password of admin user')
    parser.add_argument('-c', '--cnap_user', required=False, help='''The username (email) of the user 
        who will own the files.  If blank, will assign the files to the admin user.''')
    parser.add_argument('-e', '--expiration', required=False, help='The expiration date for the files.')
    parser.add_argument('resources', nargs='+', help='List of file paths (1 or more) in Google storage')

    args = parser.parse_args()
    arg_dict = {}
    token = get_token(args.username, args.password)
    arg_dict[TOKEN] = token

    if args.cnap_user:
        arg_dict[CNAP_USER] = args.cnap_user
    else:
        arg_dict[CNAP_USER] = args.username

    files = parse_files(args.resources)
    arg_dict[FILES] = files
    if args.expiration:
        validate_datestring(args.expiration)
        arg_dict[EXPIRY] = args.expiration
    else:
        arg_dict[EXPIRY] = None

    return arg_dict



def check_gsutil():
    cmd = 'which gsutil'
    p = subprocess.Popen(cmd, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        print('gsutil was not installed on this machine.  Please install and try again.')
        sys.exit(1) 



def get_filesize(filepath):
    '''
    filepath is a gs://... path
    Returns the file size in bytes (int)
    '''
    cmd = 'gsutil du %s' % filepath
    p = subprocess.Popen(cmd, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        print('Error when requesting file size.  Command was: %s' % cmd)
        sys.exit(1) 
    else:
        s = stdout.decode('utf-8')
        if len(s) > 0:
            contents = [x.strip() for x in s.split(' ')]
            return int(contents[0])
        else:
            print('The path (%s) did not exist.  Exiting.' % filepath)
            sys.exit(1)


def get_owner_pk(username, auth_token):
    '''
    Returns the user's primary key
    '''
    headers = {}
    headers['Authorization'] = 'Token %s' % auth_token
    headers['Content-Type']= 'application/json'
    url = DOMAIN + USERS_ENDPOINT
    r = requests.get(url, headers=headers)
    j = r.json()
    try:
        for item in j:
            if item['email'] == username:
                return item['id']
        print('User (%s) not found' % username)
        sys.exit(1)
    except: 
        print('Bad response when requesting users.')
        sys.exit(1)


def register_files(args, owner_pk):
    '''
    Makes the request to the API
    '''
    path_list = args[FILES]
    auth_token = args[TOKEN]
    url = DOMAIN + RESOURCE_ENDPOINT
    headers = {}
    headers['Authorization'] = 'Token %s' % auth_token
    headers['Content-Type']= 'application/json'
    for p in path_list:
        payload = PAYLOAD_TEMPLATE.copy()
        payload['path'] = p
        name = os.path.basename(p)
        payload['name'] = name
        size = get_filesize(p)
        payload['size'] = size
        payload['owner'] = owner_pk
        payload['expiration_date'] = args[EXPIRY]
        r = requests.post(url, data=json.dumps(payload), headers=headers)
        if r.status_code != 201:
            print('ERROR registering %s\n    Return code=%s\n    Reason:%s' % (p, r.status_code, r.text))
        else:
            print('Successfully added: %s' % p)


if __name__ == '__main__':
    check_gsutil()
    args = parse()
    auth_token = args[TOKEN]
    owner_pk = get_owner_pk(args[CNAP_USER], auth_token)
    register_files(args, owner_pk)