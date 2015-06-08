# get_run_stats.py
#
# Author: Daniel Clark, 2015

'''
This module contains functions which interact with log files on S3 to
gather runtime statistics
'''

# Get CPAC runtimes from log file
def get_cpac_runtimes(log_str):
    '''
    '''

    # Import packages
    import dateutil.parser
    import pytz

    # Init variables
    for log_line in log_str:
        if 'End - ' in log_line:
            end_line = log_line
        elif 'Elapsed run time' in log_line:
            cpac_time_line = log_line
        elif 'time of completion' in log_line:
            upl_start_line = log_line
        elif 'finished file' in log_line:
            num_files = int(log_line.split('/')[-1])
        elif 'detailed dot file' in log_line:
            subj_id = log_line.split(': ')[1].split('resting_preproc_')[1].split('/')[0]


    # Get CPAC runtime
    cpac_time = float(cpac_time_line.split(': ')[-1])

    # Get upload start time
    upl_start_time = upl_start_line.split(': ')[-1]
    upl_start_dt = dateutil.parser.parse(upl_start_time)
    upl_start_dt = pytz.utc.localize(upl_start_dt)

    # Get upload finish time
    upl_finish_time = end_line.split(' : ')[-1]
    upl_finish_dt = dateutil.parser.parse(upl_finish_time)

    # Get upload time
    upl_time = (upl_finish_dt - upl_start_dt).total_seconds()/60.0

    # Return variables
    return cpac_time, upl_time, num_files, subj_id


# Get CPAC runtimes from SGE logs
def cpac_sge_logstats(s3_prefix, str_filt, creds_path, bucket_name):
    '''
    '''

    # Import packages
    from CPAC.AWS import fetch_creds, aws_utils
    import os
    import numpy as np
    import yaml

    # Init variables
    bucket = fetch_creds.return_bucket(creds_path, bucket_name)
    log_keys = []
    log_pass = {}
    log_fail = []

    # Get the log file keys
    print 'Finding log S3 keys...'
    for key in bucket.list(prefix=s3_prefix):
        if str_filt in str(key.name):
            log_keys.append(key)

    # Get only tasks that finished
    print 'Searching for complete CPAC runs and getting runtimes...'
    for idx, key in enumerate(log_keys):
        kname = str(key.name)
        # Get log contents as a string in memory
        log_str = key.get_contents_as_string()

        # If it passed cpac running without crashing
        if 'CPAC run complete' in log_str:
            cpac_pass = True
        else:
            cpac_pass = False

        # Split log strings into list
        log_str = log_str.split('\n')

        # If it has 'End' at the end, it ran without crashing
        if 'End' in log_str[-2] and cpac_pass:
            # Get runtimes
            cpac_time, upl_time, num_files, subj = get_cpac_runtimes(log_str)
            log_pass[subj] = (cpac_time, upl_time, num_files)
        else:
            log_fail.append(kname)

        # Update status
        print '%.3f%% complete' % (100*(float(idx)/len(log_keys)))

    # Get stats
    num_subs_pass = len(log_pass)
    num_subs_fail = len(log_fail)

    cpac_times = {sub : times[0] for sub, times in log_pass.items()}
    cpac_mean = np.mean(cpac_times.values())

    upl_times = {sub : times[1] for sub, times in log_pass.items()}
    upl_mean = np.mean(upl_times.values())

    # Save times as yamls
    with open(os.path.join(os.getcwd(), 'cpac_times.yml'), 'w') as f:
        f.write(yaml.dump(cpac_times))
    with open(os.path.join(os.getcwd(), 'upl_times.yml'), 'w') as f:
        f.write(yaml.dump(upl_times))
    with open(os.path.join(os.getcwd(), 'fail_logs.yml'), 'w') as f:
        f.write(yaml.dump(log_fail))

    # Print report
    print 'Number of subjects passed: %d' % len(log_pass)
    print 'Number of subjects failed: %d' % len(log_fail)
    print 'Average CPAC run time: %.3f minutes' % cpac_mean
    print 'Average upload time: %.3f minutes' % upl_mean

    # Return variables
    return cpac_times, upl_times


# Histogram of runtimes
def plot_runtimes_hist(times_dict, num_bins):
    '''
    '''

    # Import packages
    import matplotlib.pyplot as plt
    import numpy as np
    import os

    # Init variables
    times = times_dict.values()
    hist, bins = np.histogram(times, bins=num_bins)

    # Plot histogram
    width = 0.7 * (bins[1] - bins[0])
    center = (bins[:-1] + bins[1:]) / 2
    plt.bar(center, hist, align='center', width=width)

    # Save fig
    plt.savefig(os.path.join(os.getcwd(), 'histogram.png'))
