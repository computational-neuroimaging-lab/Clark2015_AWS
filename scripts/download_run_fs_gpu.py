# download_run_fs_gpu.py
#
# Authors: Daniel Clark, John Pellman, 2015

'''
This module downloads anatomical data from S3 and runs freesurfer's
recon-all -all command on it with CUDA binaries.

Usage:
    python download_run_fs_gpu <index> <local_dir>
'''

# Setup log file
def setup_logger(logger_name, log_file, level, to_screen=False):
    '''
    Function to initialize and configure a logger that can write to file
    and (optionally) the screen.

    Parameters
    ----------
    logger_name : string
        name of the logger
    log_file : string
        file path to the log file on disk
    level : integer
        indicates the level at which the logger should log; this is
        controlled by integers that come with the python logging
        package. (e.g. logging.INFO=20, logging.DEBUG=10)
    to_screen : boolean (optional)
        flag to indicate whether to enable logging to the screen

    Returns
    -------
    logger : logging.Logger object
        Python logging.Logger object which is capable of logging run-
        time information about the program to file and/or screen
    '''

    # Import packages
    import logging

    # Init logger, formatter, filehandler, streamhandler
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s : %(message)s')

    # Write logs to file
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Write to screen, if desired
    if to_screen:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    # Return the logger
    return logger


# Form list of anatomical s3 keys
def return_anat_dict(bucket, prefix):
    '''
    Function to create and return an dictionary from an S3 bucket
    prefix, where the key is the subject unique id and the value is the
    S3 key filepath

    Parameters
    ----------
    bucket : boto.s3.bucket.Bucket instance
            an instance of the boto S3 bucket class to download from
    prefix : string
        S3 bucket prefix to parse for anatomical data in

    Returns
    -------
    key_dict : dictionary
        dictionary of unique subject id's as keys and S3 key filepaths
        as values
    '''

    # Init variables
    key_list = []
    key_dict = {}

    # Check prefix
    if not prefix.endswith('/'):
        prefix = prefix + '/'

    # Gather all anatomical files
    for key in bucket.list(prefix=prefix):
        key_name = str(key.name)
        if 'anat' in key_name:
            key_list.append(key_name)
            print 'Adding %s to list...' % key_name

    # Create subject dictionary
    for key_idx, key_name in enumerate(key_list):

        # Grab unique subj/session as id
        key_suffix = key_name.replace(prefix, '')
        subj_id = '-'.join(key_suffix.split('/')[:2])

        # Add key, val to dictionary
        key_dict[subj_id] = key_name

    # Return dictionary
    return key_dict


# Main routine
def main(index, local_dir):
    '''
    Function to download an anatomical dataset from S3 and process it
    through Freesurfer's recon-all command, then upload the data back
    to S3

    Parameters
    ----------
    index : integer
        the index of the subject to process
    local_dir : string
        filepath to the local directory to store the input and
        processed outputs
    '''

    # Import packages
    import boto
    import logging
    import os
    import subprocess
    from CPAC.AWS import aws_utils, fetch_creds
    import pycuda.autoinit
    import pycuda.driver as cuda
    from multiprocessing import Process

    # Init variables
    creds_path = '/home/ubuntu/secure-creds/aws-keys/fcp-indi-keys2.csv'
    bucket = fetch_creds.return_bucket(creds_path, 'fcp-indi')
    prefix = 'data/Projects/CORR/RawData/IBA_TRT/'
    dl_dir = os.path.join(local_dir, 'inputs')
    subjects_dir = os.path.join(local_dir, 'subjects')

    # Setup logger
    fs_log_path = os.path.join(local_dir, 'download_run_fs_%d.log' % index)
    fs_log = setup_logger('fs_log', fs_log_path, logging.INFO, to_screen=True)

    # Make input and subject dirs
    if not os.path.exists(dl_dir):
        os.makedirs(dl_dir)

    if not os.path.exists(subjects_dir):
        os.makedirs(subjects_dir)

    # Get S3 anatomical paths dictionary
    anat_dict = return_anat_dict(bucket, prefix)

    # Get list of unique subject ids to download
    key_list = sorted(anat_dict.keys())

    # Determine number of GPUs
    num_gpus=cuda.Device.count()
    subj_id=[]
    proc=[]
    upload_proc=[]
    for inst in range(num_gpus):

        # Set environment variable.
        os.environ['FREESURFER_CUDA_DEVICE'] = str(inst) 
        # Get the index of the subject to be run.
        subj_index = num_gpus*index + inst
        # Extract subject of interest
        subj_id.append(key_list[subj_index])
        s3_path = anat_dict[subj_id[inst]]

        # Download data
        fs_log.info('Downloading %s...' % s3_path)
        s3_key = bucket.get_key(s3_path)
        s3_filename = os.path.basename(s3_path)
        dl_filename = os.path.join(dl_dir, subj_id[inst], s3_filename)
        # Make folders if need be
        dl_dirs = os.path.dirname(dl_filename)
        if not os.path.exists(dl_dirs):
            os.makedirs(dl_dirs)
        s3_key.get_contents_to_filename(dl_filename)

        # Execute recon-all
        cmd_list = ['recon-all', '-use_gpu','-openmp','8', '-time', '-qcache',
                    '-i', dl_filename, '-subjid', subj_id[inst], '-all']
        cmd_str = ' '.join(cmd_list)
        fs_log.info('Executing %s...' % cmd_str)
        # Use subprocess to send command and communicate outputs
        proc.append(subprocess.Popen(cmd_list))

    # Run uploads with multiprocessing's Process
    for inst in range(0,num_gpus):
        proc[inst].wait()

        # Gather processed data
        fs_log.info('Gathering outputs for upload to S3...')
        upl_list = []
        subj_dir = os.path.join(subjects_dir, subj_id[inst])
        for root, dirs, files in os.walk(subj_dir):
            if files:
                upl_list.extend([os.path.join(root, fl) for fl in files])
        # Update log with upload info
        fs_log.info('Gathered %d files for upload to S3' % len(upl_list))

        # Build upload list
        upl_prefix = os.path.join(prefix.replace('RawData', 'Outputs'),
                                  'freesurfer_gpu', subj_id[inst])
        s3_upl_list = [upl.replace(subj_dir, upl_prefix) for upl in upl_list]

        # Upload to S3
        upload_proc.append(Process(target=aws_utils.s3_upload,
                                   args=(bucket, upl_list, s3_upl_list)))
        upload_proc[inst].start()

    # Check that uploading has finished. 
    for inst in range(0,num_gpus):
        upload_proc[inst].join()


# Make executable
if __name__ == '__main__':

    # Import packages
    import sys

    # Init variables
    index = int(sys.argv[1])-1
    local_dir = sys.argv[2]

    main(index, local_dir)
