# spot-model/utils.py
#
# Author: Daniel Clark, 2015

'''
This module contains various utilities for the modules and scripts in
this folder or package
'''

# Build data frame
def build_big_df(av_zone_dir):
    '''
    Function to parse and merge the simulation results from the
    *_sim and *_stats files into one big data frame based on the
    availability zone directory provided; it saves this to a csv

    Parameters
    ----------
    av_zone_dir : string
        file path to the directory containing the simulation results

    Returns
    -------
    big_df : pandas.DatFrame object
        a merged dataframe with all of the stats for the simulation
    '''

    # Import packages
    from spot_price_model import spothistory_from_dataframe
    import glob
    import numpy as np
    import os
    import pandas as pd

    # Init variables
    df_list = []
    av_zone = av_zone_dir.split('/')[-1]
    csvs = glob.glob(os.path.join(av_zone_dir, '*_stats.csv'))

    # Print av zone of interest being created
    print av_zone
    spot_history = spothistory_from_dataframe('spot_history/merged_dfs.csv', 'c3.8xlarge', 'Linux/UNIX', av_zone)

    # Iterate through csvs
    for stat_csv in csvs:
        # Get pattern to find sim dataframe
        csv_pattern = stat_csv.split('_stats.csv')[0]
        sim_csv = csv_pattern + '_sim.csv'
        stat_df = pd.DataFrame.from_csv(stat_csv)
        sim_df = pd.DataFrame.from_csv(sim_csv)

        # Extract params from filename
        fp_split = csv_pattern.split('-jobs')
        bid_ratio = float(fp_split[1][1:].split('-bid')[0])
        bid_price = bid_ratio*spot_history.mean()

        ### Download time fix ###
        # *Note the CPAC, ANTs, and Freesurfer csv outputs need this
        # CPAC pipeline params
        jobs_per = 3
        down_rate = 20
        out_gb_dl = 2.3
        down_gb_per_sec = down_rate/8.0/1024.0
        # Variables for download time fix
        num_ds = int(fp_split[0].split('/')[-1].split('_')[-1])
        num_nodes = min(np.ceil(float(num_ds)/jobs_per), 20)
        num_iter = np.ceil(num_ds/float((jobs_per*num_nodes)))
        num_jobs_n1 = ((num_iter-1)*num_nodes*jobs_per)
        res_xfer_out = (num_ds-num_jobs_n1)*(out_gb_dl/down_gb_per_sec)
        # Fix download time
        #stat_df['Download time'] += res_xfer_out/60.0 

        # Add to stat df
        len_df = len(stat_df)
        stat_df['Sim index'] = sim_df.index
        stat_df['Av zone'] = pd.Series([av_zone]*len_df, index=stat_df.index)
        stat_df['Bid ratio'] = pd.Series([bid_ratio]*len_df, index=stat_df.index)
        stat_df['Bid price'] = pd.Series([bid_price]*len_df, index=stat_df.index)
        stat_df['Num datasets'] = pd.Series([num_ds]*len_df, index=stat_df.index)
        stat_df['Start time'] = sim_df['Start time']
        stat_df['Interrupts'] = sim_df['Interrupts']
        stat_df['First Iter Time'] = sim_df['First Iter Time']

        # Add to dataframe list
        df_list.append(stat_df)

    # Status update
    print 'done making df list, now concat to big df...'
    big_df = pd.concat(df_list, ignore_index=True)

    # Write to disk as csv
    print 'Saving to disk...'
    big_df.to_csv('./%s.csv' % av_zone)
    print 'done writing!'

    # Return dataframe
    return big_df


# Build list of processes to use in multi-proc
def build_proc_list(zones_basedir):
    '''
    Function to build a list of build_big_df processes from a directory
    of availability zones folders

    Parameters
    ----------
    zones_basedir : string
        base directory where the availability zone folders are residing

    Returns
    -------
    proc_list : list
        a list of multiprocessing.Process objects to run the
        build_big_df function
    '''

    # Import packages
    import glob
    import os
    import pandas as pd
    from multiprocessing import Process

    # Init variables
    av_zone_fp = os.path.join(zones_basedir, '*')
    av_zones_dirs = glob.glob(av_zone_fp)

    # Build big dictionary
    proc_list = [Process(target=build_big_df, args=(av_zone_dir,)) \
                 for av_zone_dir in av_zones_dirs]

    # Return the process list
    return proc_list


# Convert spot history list to dataframe csv
def pklz_to_df(out_dir, pklz_file):
    '''
    Function to convert pklz list file to csv dataframe

    Parameters
    ----------
    out_dir : string
        filepath to the output base directory to store the dataframes
    pklz_file : string
        filepath to the .pklz file, which contains a list of
        boto spot price history objects

    Returns
    -------
    None
        this function saves the dataframe to a csv
    '''

    # Import packages
    import gzip
    import os
    import pandas as pd
    import pickle as pk
    import time

    # Init variables
    gfile = gzip.open(pklz_file)
    sh_list = pk.load(gfile)
    idx = 0

    # If the list is empty return nothing
    if len(sh_list) == 0:
        return

    # Init data frame
    df_cols = ['Timestamp', 'Price', 'Region', 'Availability zone',
               'Product', 'Instance type']
    merged_df = pd.DataFrame(columns=df_cols)

    # Iterate through histories
    for sh in sh_list:
        timestamp = str(sh.timestamp)
        price = sh.price
        reg = str(sh.region).split(':')[-1]
        av_zone = str(sh.availability_zone)
        prod = str(sh.product_description)
        inst = str(sh.instance_type)
        df_entry = [timestamp, price, reg, av_zone, prod, inst]
        merged_df.loc[idx] = df_entry
        idx += 1
        print '%d/%d' % (idx, len(sh_list))

    # Write out merged dataframe
    out_csv = os.path.join(out_dir, reg, prod.replace('/', '-'), inst, str(time.time()) + '.csv')
    csv_dir = os.path.dirname(out_csv)

    # Check if folders exists
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

    print 'Done merging, writing out to %s...' % out_csv
    merged_df.to_csv(out_csv)


# Run jobs in parallel
def run_in_parallel(proc_list, num_cores):
    '''
    Function to kick off a list of processes in parallel, guaranteeing
    that a fixed number of cores or less is running at all times

    Parameters
    ----------
    proc_list : list
        a list of multiprocessing.Process objects
    num_cores : integer
        the number of cores or processes to run at once

    Returns
    -------
    None
        there is no return value for this function
    '''

    # Import packages
    import time

    # Init variables
    idx = 0
    job_queue = []

    # While loop for when jobs are still running
    while idx < len(proc_list):
        if len(job_queue) == 0 and idx == 0:
            idc = idx
            for p in proc_list[idc:idc+num_cores]:
                p.start()
                job_queue.append(p)
                idx += 1
        else:
            for job in job_queue:
                if not job.is_alive():
                    print 'found dead job', job
                    loc = job_queue.index(job)
                    del job_queue[loc]
                    if idx < len(proc_list):
                        proc_list[idx].start()
                    else:
                        break
                    job_queue.append(proc_list[idx])
                    idx += 1
            time.sleep(2)


# Print status of file progression in loop
def print_loop_status(itr, full_len):
    '''
    Function to print the current percentage completed of a loop
    Parameters
    ----------
    itr : integer
        the current iteration of the loop
    full_len : integer
        the full length of the loop
    Returns
    -------
    None
        the function prints the loop status, but doesn't return a value
    '''

    # Print the percentage complete
    per = 100*(float(itr)/full_len)
    print '%d/%d\n%f%% complete' % (itr, full_len, per)


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


# Make executable
if __name__ == '__main__':

    # Import packages
    import sys

    # Grab az_zone folders base
    zones_basedir = str(sys.argv[1])

    # Call main
    proc_list = build_proc_list(zones_basedir)

    #build_big_df('~/Documents/projects/Clark2015_AWS/spot-model/out/us-east-1a')
    # Run in parallel
    run_in_parallel(proc_list, 6)
