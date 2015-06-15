# build_big_df.py
#
# Author: Daniel Clark, 2015

'''
'''

# Build data frame
def build_big_df(av_zone_dir):
    '''
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
        stat_df['Download time'] += res_xfer_out/60.0

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
    '''

    # Import packages
    import glob
    import pandas as pd
    from multiprocessing import Process

    # Init variables
    av_zone_fp = zones_basedir + '*'
    av_zones_dirs = glob.glob(av_zone_fp)

    # Build big dictionary
    proc_list = [Process(target=build_big_df, args=(av_zone_dir,)) \
              for av_zone_dir in av_zones_dirs]

    return proc_list


# Run jobs in parallel
def run_in_parallel(proc_list, num_cores):
    '''
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
                    proc_list[idx].start()
                    job_queue.append(proc_list[idx])
                    idx += 1
            time.sleep(2)


# Make executable
if __name__ == '__main__':

    # Grab az_zone folders base
    zones_basedir = '/home/dclark/Documents/projects/Clark2015_AWS/spot-model/fs/'

    # Call main
    proc_list = build_proc_list(zones_basedir)

    # Run in parallel
    run_in_parallel(proc_list, 6)
