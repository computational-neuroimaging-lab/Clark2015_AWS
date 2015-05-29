# act_run.py
#
# Author: Daniel Clark, 2015

'''
This module contains functions which run antsCorticalThickness and ROI
extractions and then uploads them to S3
'''

# Create the ACT nipype workflow
def create_workflow(wf_base_dir, input_anat, oasis_path):
    '''
    Method to create the nipype workflow that is executed for
    preprocessing the data

    Parameters
    ----------
    wf_base_dir : string
        filepath to the base directory to run the workflow
    input_anat : string
        filepath to the input file to run antsCorticalThickness.sh on
    oasis_path : string
        filepath to the oasis

    Returns
    -------
    wf : nipype.pipeline.engine.Workflow instance
        the workflow to be ran for preprocessing
    '''

    # Import packages
    from act_interface import antsCorticalThickness
    import nipype.interfaces.io as nio
    import nipype.pipeline.engine as pe
    import nipype.interfaces.utility as util
    from nipype.interfaces.utility import Function
    from nipype import logging as np_logging
    from nipype import config
    import os

    # Init variables
    oasis_trt_20 = os.path.join(oasis_path,
                                'OASIS-TRT-20_jointfusion_DKT31_CMA_labels_in_OASIS-30.nii')

    # Setup nipype workflow
    if not os.path.exists(wf_base_dir):
        os.makedirs(wf_base_dir)
    wf = pe.Workflow(name='thickness_workflow')
    wf.base_dir = wf_base_dir

    # Init log directory
    log_dir = wf_base_dir

    # Define antsCorticalThickness node
    thickness = pe.Node(antsCorticalThickness(), name='thickness')

    # Set antsCorticalThickness inputs
    thickness.inputs.dimension = 3
    thickness.inputs.segmentation_iterations = 1
    thickness.inputs.segmentation_weight = 0.25
    thickness.inputs.input_skull = input_anat #-a
    thickness.inputs.template = oasis_path + 'T_template0.nii.gz' #-e
    thickness.inputs.brain_prob_mask = oasis_path + \
                                       'T_template0_BrainCerebellumProbabilityMask.nii.gz'  #-m
    thickness.inputs.brain_seg_priors = oasis_path + \
                                        'Priors2/priors%d.nii.gz'  #-p
    thickness.inputs.intensity_template = oasis_path + \
                                          'T_template0_BrainCerebellum.nii.gz'  #-t
    thickness.inputs.extraction_registration_mask = oasis_path + \
                                                    'T_template0_BrainCerebellumExtractionMask.nii.gz'  #-f
    thickness.inputs.out_prefix = 'OUTPUT_' #-o
    thickness.inputs.keep_intermediate_files = 0 #-k

    # Node to run ANTs 3dROIStats
    ROIstats = pe.Node(util.Function(input_names=['mask','thickness_normd'], 
                                     output_names=['roi_stats_file'], 
                                     function=roi_func),
                       name='ROIstats')
    wf.connect(thickness, 'cortical_thickness_normalized', 
               ROIstats, 'thickness_normd')
    ROIstats.inputs.mask = oasis_trt_20

    # Create datasink node
    datasink = pe.Node(nio.DataSink(), name='sinker')
    datasink.inputs.base_directory = wf_base_dir

    # Connect thickness outputs to datasink
    wf.connect(thickness, 'brain_extraction_mask', 
               datasink, 'output.@brain_extr_mask')
    wf.connect(thickness, 'brain_segmentation', 
               datasink, 'output.@brain_seg')
    wf.connect(thickness, 'brain_segmentation_N4', 
               datasink, 'output.@brain_seg_N4')
    wf.connect(thickness, 'brain_segmentation_posteriors_1', 
               datasink, 'output.@brain_seg_post_1')
    wf.connect(thickness, 'brain_segmentation_posteriors_2', 
               datasink, 'output.@brain_seg_post_2')
    wf.connect(thickness, 'brain_segmentation_posteriors_3', 
               datasink, 'output.@brain_seg_post_3')
    wf.connect(thickness, 'brain_segmentation_posteriors_4', 
               datasink, 'output.@brain_seg_post_4')
    wf.connect(thickness, 'brain_segmentation_posteriors_5', 
               datasink, 'output.@brain_seg_post_5')
    wf.connect(thickness, 'brain_segmentation_posteriors_6', 
               datasink, 'output.@brain_seg_post_6')
    wf.connect(thickness, 'cortical_thickness', 
               datasink, 'output.@cortical_thickness')
    wf.connect(thickness, 'cortical_thickness_normalized', 
               datasink,'output.@cortical_thickness_normalized')
    # Connect ROI stats output text file to datasink
    wf.connect(ROIstats, 'roi_stats_file', datasink, 'output.@ROIstats')

    # Setup crashfile directory and logging
    wf.config['execution'] = {'hash_method': 'timestamp', 
                              'crashdump_dir': '/home/ubuntu/crashes'}
    config.update_config({'logging': {'log_directory': log_dir, 
                                      'log_to_file': True}})
    np_logging.update_logging(config)

    # Return the workflow
    return wf


# Mean ROI stats function
def roi_func(mask, thickness_normd):
    '''
    Method to run 3dROIstats on an input image, thickness_normd, using
    a mask, mask The output is written to the current working directory
    as 'ROIstats.txt'

    Parameters
    ----------
    mask : string
        filepath to the mask to be used
    thickness_normd : string
        filepath to the input image

    Returns
    -------
    roi_stats_file : string
        the filepath to the generated ROIstats.txt file
    '''

    # Import packages
    import os

    # Set command and execute
    cmd = '3dROIstats -mask ' + mask + ' ' + thickness_normd + ' > ' + os.getcwd() + '/ROIstats.txt'
    os.system(cmd)

    # Get the output
    roi_stats_file = os.path.join(os.getcwd(), 'ROIstats.txt')

    # Return the filepath to the output
    return roi_stats_file


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
    through ANTS antsCorticalThickness.sh script, then upload the data back
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
    import time
    from CPAC.AWS import aws_utils, fetch_creds

    # Init variables
    creds_path = '/home/ubuntu/secure-creds/aws-keys/fcp-indi-keys2.csv'
    # Oasis template paths
    oasis_path = '/home/ubuntu/OASIS-30_Atropos_template/'
    # Bucket and S3 dataset prefix
    bucket = fetch_creds.return_bucket(creds_path, 'fcp-indi')
    prefix = 'data/Projects/CORR/RawData/IBA_TRT/'
    # Local dirs for working and download
    dl_dir = os.path.join(local_dir, 'inputs')

    # Setup logger
    act_log_path = '/home/ubuntu/run_act_%d.log' % index
    act_log = setup_logger('act_log', act_log_path, logging.INFO, to_screen=True)

    # Make input and workdirs
    if not os.path.exists(dl_dir):
        os.makedirs(dl_dir)

    # Get S3 anatomical paths dictionary
    anat_dict = return_anat_dict(bucket, prefix)

    # Get lis of unique subject ids to download
    key_list = sorted(anat_dict.keys())

    # Extract subject of interest
    subj_id = key_list[index]
    s3_path = anat_dict[subj_id]

    # Init working dir
    working_dir = os.path.join(local_dir, '%s_act_workdir' % subj_id)
    if not os.path.exists(working_dir):
        os.makedirs(working_dir)

    # Download data
    act_log.info('Downloading %s...' % s3_path)
    s3_key = bucket.get_key(s3_path)
    s3_filename = os.path.basename(s3_path)
    dl_filename = os.path.join(dl_dir, subj_id, s3_filename)

    # Make folders if need be
    dl_dirs = os.path.dirname(dl_filename)
    if not os.path.exists(dl_dirs):
        os.makedirs(dl_dirs)
    s3_key.get_contents_to_filename(dl_filename)

    # Create the nipype workflow
    act_wf = create_workflow(working_dir, dl_filename, oasis_path)

    # Run the workflow
    act_log.info('Running the workflow...')
    # Start timing
    start = time.time()
    act_wf.run()
    # Finish timing
    fin = time.time()
    act_log.info('Completed workflow!')

    # Log finish and total computation time
    elapsed = (fin - start)/60.0
    act_log.info('Total time running is: %f minutes' % elapsed)

    # Gather processed data
    act_log.info('Gathering outputs for upload to S3...')
    upl_list = []
    for root, dirs, files in os.walk(working_dir):
        if files:
            upl_list.extend([os.path.join(root, fl) for fl in files])
    # Update log with upload info
    act_log.info('Gathered %d files for upload to S3' % len(upl_list))

    # Build upload list
    upl_prefix = os.path.join(prefix.replace('RawData', 'Outputs'),
                              'IBA_TRT', 'ants', subj_id)
    s3_upl_list = [upl.replace(working_dir, upl_prefix) for upl in upl_list]

    # Upload to S3
    aws_utils.s3_upload(bucket, upl_list, s3_upl_list)


# Run main by default
if __name__ == '__main__':

    # Import packages
    import sys

    # Init variables
    index = int(sys.argv[1])-1
    local_dir = sys.argv[2]

    main(index, local_dir)
