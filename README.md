### A place to keep various helpful scripts

- `dropbox_transfer.py`: a utility for uploading or downloading files to/from Dropbox.
    - Requires an application API key available at the Dropbox developers site (https://www.dropbox.com/developers)
    - Requires Dropbox python SDK (`pip install dropbox`)
    - Uses Python3 (used with 3.5, have not tried later versions)
    - Run `python3 dropbox_transfer.py -h` for help or command line args
    
- `register_files.py`: a utility for registering files with CNAP.  Does not perform up/downloads.
    - Requires python3 and gsutil to be installed.  No other python3 dependencies required.
    - Run `python3 register_files.py -h` for help.
    - Requires an admin account on the CNAP application
    - If you want to register files for another user, they must be registered before using.
    - Files must already exist in Google storage, and must *not* be assigned to another user.
        - This prevents conflicts if two users are registered as "owning" the same file.
        
- `cromwell_headless_submit.py`: a script for interacting with Cromwell for job submission, querying, and aborting.
    - Run `python3 cromwell_headless_submit.py -h` for args help
