import argparse
import json
import requests
import io
import os
import sys
 

CROMWELL_SERVER_URL = 'http://{ip}:{port}'
API_VERSION = 'v1'
QUERY = 'query'
SUBMIT = 'submit'
ABORT = 'abort'


# These parameters are unlikely to change often unless Cromwell spec changes.
DEFAULT_CONFIG = {
    'submit_endpoint' : '/api/workflows/{api_version}',
    'status_endpoint' : '/api/workflows/{api_version}/{job_uuid}/status',
    'abort_endpoint': '/api/workflows/{api_version}/{job_uuid}/abort',
    'workflow_type' : 'WDL',
    'workflow_type_version' : 'draft-2'
}


def parse_cl_args():
    parser = argparse.ArgumentParser()
    
    # args that are used regardless of subcommand:
    parser.add_argument('--ip', required=True, help='IP address of Cromwell server')
    parser.add_argument('--port', required=False, default=8000, type=int, help='Port where the Cromwell server listens')

    # Create multiple subparsers for submission, query, etc.
    subparsers = parser.add_subparsers(help='Subcommand help', dest='subcommand')
    subparsers.required = True # hack for python pre-3.7

    # Options/args for submitting new jobs
    submit_parser = subparsers.add_parser(SUBMIT, help="Submit a new job to Cromwell")
    submit_parser.add_argument('-i', '--input-json', required=True, help='JSON-format WDL inputs')
    submit_parser.add_argument('-zip', '--dependencies-zip', required=False, help='ZIP archive for other WDL files.')
    submit_parser.add_argument('-zone', required=False, default='us-east4-c', help='Zone in which to execute the job')
    submit_parser.add_argument('main_wdl', help='The main WDL file')

    # Options/args for querying a job for its status:
    query_parser = subparsers.add_parser(QUERY, help="Query Cromwell for job status")
    query_parser.add_argument('-i', '--cromwell-id', required=True, help='The Cromwell UUID for the job')

    # Options/args for aborting jobs:
    query_parser = subparsers.add_parser(ABORT, help="Abort a job")
    query_parser.add_argument('-i', '--cromwell-id', required=True, help='The Cromwell UUID for the job')
    
    args = parser.parse_args()
    return vars(args)


def submit_job(args):
    '''
    input_json is a dict
    '''

    # load the inputs as a dict:
    try:
        j = json.load(open(args['input_json']))
    except FileNotFoundError:
        print('Could not locate %s.  Check the path.' % args['input_json'])
        sys.exit(1)
    except json.decoder.JSONDecodeError as ex:
        print('JSON was not properly formatted.  Error was:')
        print(ex)    
        sys.exit(1)

    # pull together the components of the POST request to the Cromwell server
    submission_endpoint = DEFAULT_CONFIG['submit_endpoint'].format(api_version = API_VERSION)
    submission_url = CROMWELL_SERVER_URL.format(ip=args['ip'], port=args['port']) + submission_endpoint

    payload = {}
    payload = {'workflowType': DEFAULT_CONFIG['workflow_type'], \
        'workflowTypeVersion': DEFAULT_CONFIG['workflow_type_version']
    }

    # Create an options dict so we can specify the zones and possibly other configuration params:
    options_json = {}
    options_json['default_runtime_attributes'] = {'zones': args['zone']}

    # Give these file-like handles with the BytesIO interface:
    options_io = io.BytesIO(json.dumps(options_json).encode('utf-8'))
    json_inputs_io = io.BytesIO(json.dumps(j).encode('utf-8'))
    files = {
        'workflowOptions': options_io, 
        'workflowInputs': json_inputs_io
    }

    # Load-in the main WDL script:
    files['workflowSource'] =  open(args['main_wdl'], 'rb')

    # Check if there were other WDL files, packaged as a zip:
    if args['dependencies_zip'] and os.path.exists(args['dependencies_zip']):
        files['workflowDependencies'] = open(zip_archive, 'rb')

    # start the job:
    try:
        response = requests.post(submission_url, data=payload, files=files)
    except Exception as ex:
        print('An exception was raised when requesting Cromwell server:')
        print(ex)
        message = 'An exception occurred when trying to submit a job to Cromwell. \n'
        return None

    response_json = json.loads(response.text)
    if response.status_code == 201:
        if response_json['status'] == 'Submitted':
            job_id = response_json['id']
            print('Job was submitted to Cromwell and was given job ID=%s' % job_id)
            return None
        else:
            # In case we get other types of responses, inform the admins:
            message = 'Job was submitted, but received an unexpected response from Cromwell:\n'
            message += response.text
            print(message)
            return None
    else:
        message = 'Did not submit job-- status code was %d, and response text was: %s' % (response.status_code, response.text)
        print(message)
        return None


def query_job_status(args):
    # pull together the components of the GET request to the Cromwell server
    endpoint = DEFAULT_CONFIG['status_endpoint'].format(api_version = API_VERSION, job_uuid=args['cromwell_id'])
    request_url = CROMWELL_SERVER_URL.format(ip=args['ip'], port=args['port']) + endpoint
    response = requests.get(request_url)
    if response.status_code == 200:
        print(response.text)
    else:
        print('Status request was not successful.  Received status code %d' % response.status_code)


def abort_job(args):
    # pull together the components of the POST request to the Cromwell server
    endpoint = DEFAULT_CONFIG['abort_endpoint'].format(api_version = API_VERSION, job_uuid=args['cromwell_id'])
    request_url = CROMWELL_SERVER_URL.format(ip=args['ip'], port=args['port']) + endpoint
    response = requests.post(request_url)
    if response.status_code == 200:
        print(response.text)
    else:
        print('Abort request was not successful.  Received status code %d' % response.status_code)


if __name__ == '__main__':
    args = parse_cl_args()

    if args['subcommand'] == QUERY:
        query_job_status(args)
    elif args['subcommand'] == SUBMIT:
        submit_job(args)
    elif args['subcommand'] == ABORT:
        abort_job(args)
    else:
        print('Unrecognized subcommand.')