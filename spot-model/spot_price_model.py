# spot_price_model.py
#
# Contributing authors: Daniel Clark, Cameron Craddock, 2015

'''
This module contains functions which return the total duration, cost,
expected failure time, expected wait time, and probability of failure
for a job submission to an AWS EC2 SPOT cluster
'''

# Calculate cost over interval
def calculate_cost(start_time, uptime_seconds, interp_history, interrupted=False):
    '''
    '''

    # Import packages
    import numpy as np
    import pandas as pd
    import datetime

    # Init variables
    pay_periods = np.floor(uptime_seconds/3600.0)
    end_time = start_time + datetime.timedelta(seconds=uptime_seconds)
    hour_seq = pd.date_range(start_time, periods=pay_periods, freq='H')
    hourly_series = interp_history[hour_seq]

    # Sum up all but last hour price if interrupted
    total_cost = hourly_series.sum()

    # If the user ran residual time without interrupt after last hour
    if not interrupted:
        if len(hourly_series) == 0:
            print 'rawdog'
        residual_time = end_time - (hour_seq[-1]+datetime.timedelta(hours=1))
        residual_hours = residual_time.total_seconds()/3600.0
        total_cost += hourly_series[-1]*residual_hours

    # Return the total cost
    return total_cost


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


# Lookup tables for pricing for EBS
def get_ebs_costs(av_zone):
    '''
    Data transfer into EC2 from the internet is free (all regions)

    Region name - Location mapping
    ------------------------------
    us-east-1 - N. Virginia
    us-west-1 - N. California
    us-west-2 - Oregon
    eu-west-1 - Ireland
    eu-central-1 - Frankfurt
    ap-southeast-1 - Singapore
    ap-southeast-2 - Sydney
    ap-northeast-1 - Tokyo
    sa-east-1 - Sao Paulo

    References
    ----------
    EBS pricing: http://aws.amazon.com/ebs/pricing/
    '''

    # Import packages

    # Init variables
    region = av_zone[:-1]

    # EBS general purpose storage
    ebs_gen_purp = {'us-east-1' : 0.1,
                    'us-west-1' : 0.12,
                    'us-west-2' : 0.1,
                    'eu-west-1' : 0.11,
                    'eu-central-1' : 0.119,
                    'ap-southeast-1' : 0.12,
                    'ap-southeast-2' : 0.12,
                    'ap-northeast-1' : 0.12,
                    'sa-east-1' : 0.19}
    # EBS magnetic storage (plus same price per million I/O requests)
    ebs_mag = {'us-east-1' : 0.05,
               'us-west-1' : 0.08,
               'us-west-2' : 0.05,
               'eu-west-1' : 0.055,
               'eu-central-1' : 0.059,
               'ap-southeast-1' : 0.08,
               'ap-southeast-2' : 0.08,
               'ap-northeast-1' : 0.08,
               'sa-east-1' : 0.12}

    # Get costs for downloading data from EC2 (up to 10TB/month), in $/GB
    ec2_xfer_out = {'us-east-1' : 0.09,
                    'us-west-1' : 0.09,
                    'us-west-2' : 0.09,
                    'eu-west-1' : 0.09,
                    'eu-central-1' : 0.09,
                    'ap-southeast-1' : 0.12,
                    'ap-southeast-2' : 0.14,
                    'ap-northeast-1' : 0.14,
                    'sa-east-1' : 0.25}


# Lookup tables for pricing for EBS
def get_s3_costs(av_zone, in_gb, out_gb):
    '''
    Data transfer to S3 from anywhere is free (all regions)
    Data transfer from S3 to EC2 in same region is free (all regions)

    References
    ----------
    S3 pricing: http://aws.amazon.com/s3/pricing/
    '''

    # Init variables
    region = av_zone[:-1]

    # How many output files get generated per input file
    # Assume ~50
    out_ratio = 50

    # S3 standard storage (up to 1TB/month), units of $/GB-month
    s3_stor = {'us-east-1' : 0.03,
               'us-west-1' : 0.033,
               'us-west-2' : 0.03,
               'eu-west-1' : 0.03,
               'eu-central-1' : 0.0324,
               'ap-southeast-1' : 0.03,
               'ap-southeast-2' : 0.033,
               'ap-northeast-1' : 0.033,
               'sa-east-1' : 0.0408}

    # Get costs for downloading data from S3 (up to 10TB/month), in $/GB
    s3_xfer_out = {'us-east-1' : 0.09,
                   'us-west-1' : 0.09,
                   'us-west-2' : 0.09,
                   'eu-west-1' : 0.09,
                   'eu-central-1' : 0.09,
                   'ap-southeast-1' : 0.12,
                   'ap-southeast-2' : 0.14,
                   'ap-northeast-1' : 0.14,
                   'sa-east-1' : 0.25}

    # Request pricing (put vs get on S3)
    # Put - $/1,000 reqs (upload)
    # Get - $/10,000 reqs (download)
    s3_reqs = {'us-east-1' : {'put' : 0.005, 'get' : 0.004},
               'us-west-1' : {'put' : 0.0055, 'get' : 0.0044},
               'us-west-2' : {'put' : 0.005, 'get' : 0.004},
               'eu-west-1' : {'put' : 0.005, 'get' : 0.004},
               'eu-central-1' : {'put' : 0.0054, 'get' : 0.0043},
               'ap-southeast-1' : {'put' : 0.005, 'get' : 0.004},
               'ap-southeast-2' : {'put' : 0.0055, 'get' : 0.0044},
               'ap-northeast-1' : {'put' : 0.0047, 'get' : 0.0037},
               'sa-east-1' : {'put' : 0.007, 'get' : 0.0056}}

    # Return pricing for each storage, transfer, and requests
    # Assuming in_gb and out_gb stored on S3 for month
    stor_price = s3_stor[region]*(in_gb+out_gb)
    xfer_price = s3_xfer_out[region]*(out_gb)
    req_price = s3_reqs[region]['put']*(num_jobs/1000.0) + \
                s3_reqs[region]['get']*((out_ratio*num_jobs)/10000.0)

    # Sum of storage, transfer, and requests
    s3_price = stor_price + xfer_price + req_price

    # Return s3 costs
    return s3_price


# Find how often a number of jobs fails and its total cost
def simulate_market(start_time, spot_history, interp_history,
                    proc_time, num_iter, bid_price):
    '''
    Function to find the total execution time, cost, and number of interrupts
    for a given job submission and bid price

    Parameters
    ----------
    start_time : pandas.tslib.Timestamp object
        the time to start the simulation from
    spot_history : pandas.core.series.Series object
        timeseries of spot prices recorded from AWS
    interp_history : pandas.core.series.Series object
        interpolated spot price history to one second resolution
    proc_time : float
         the time to process one job iteration (in seconds)
    num_iter : integer
        the number of job iterations or waves to run
    bid_price : float
        the spot bid price in dollars per hour

    Returns
    -------
    total_runtime : float
        the total number of seconds all of the nodes were up running
    total_wait : float
        the total number of seconds spent waiting for the spot price
        to come down below bid
    total_cost : float
        the total number of dollars the entire job submission cost
    num_interrupts : integer
        the number of times the job submission was interrupted
    '''

    # Import packages
    import datetime
    import numpy as np

    # Init variables
    total_runtime = 0
    total_wait = 0
    total_cost = 0
    num_interrupts = 0

    # Get first spot history start time
    current_indices = spot_history.index >= start_time
    spot_history_start = min(spot_history.index[current_indices])

    # Init remaining rumtime
    remaining_runtime = proc_time*num_iter

    # While there is time left running
    while remaining_runtime > 0:
        # Get only current spot history
        curr_spot_history = spot_history[spot_history_start:]

        # Get instance-boot-up price (per hour price at start time)
        start_price = interp_history[start_time]

        # If start price is greater than bid, interrupt immediately
        if start_price >= bid_price:
            uptime_seconds = 0
            interrupt_time = start_time
        else:
            # Otherwise, start instances and charge for launch
            total_cost += start_price

            # Find interrupts
            interrupt_condition = curr_spot_history >= bid_price

            # Find timestamp where first interrupt occured
            if np.any(interrupt_condition):
                interrupt_time = min(curr_spot_history.index[interrupt_condition])
            else:
                interrupt_time = spot_history.index[-1]

            # Calculate total up-and-running time
            uptime = interrupt_time - start_time
            uptime_seconds = uptime.total_seconds()

        # See if job completed
        if uptime_seconds > remaining_runtime:
            # Add remaining runtime to execution time
            total_runtime += remaining_runtime

            # Add remaining runtime costs
            total_cost += calculate_cost(start_time, remaining_runtime, interp_history)

            # Clear remaining run time
            remaining_runtime = 0

        # Job suspended until price returns below bid
        else:
            # Increment it as an interrupt if we were running before hand
            if uptime_seconds > 0:
                num_interrupts += 1

            # Add up time to execution time
            total_runtime += uptime_seconds

            # Add to cost
            total_cost += calculate_cost(start_time, uptime_seconds,
                                         interp_history, interrupted=True)

            # Subtract uptime from remaining runtime
            # Add back remainder of time that was interrupted (need to re-do)
            remaining_runtime = (remaining_runtime-uptime_seconds) + \
                                (uptime_seconds % proc_time)

            # Find next time the history dips below the bid price
            curr_spot_history = spot_history[interrupt_time:]
            start_condition = curr_spot_history < bid_price
            start_times = curr_spot_history.index[start_condition]
            # If we've run out of processing time
            if len(start_times) == 0 or \
               start_times[0] == spot_history.index[-1]:
                err_msg = 'Job submission could not complete due to too many ' \
                          'interrupts or starting too recently'
                raise Exception(err_msg)
            spot_history_start = min(start_times)
            # and set as the next spot time
            start_time = spot_history_start
            # 

            # And increment wait time by (next start)-(this interrupt)
            total_wait += (start_time - interrupt_time).total_seconds()

    # Return results
    return total_runtime, total_wait, total_cost, num_interrupts


# Return a time series from csv data frame
def spothistory_from_dataframe(csv_file, instance_type, product, av_zone):
    '''
    '''

    # Import packages
    import pandas as pd

    # Init variables

    # Load data frame
    print 'Loading dataframe %s...' % csv_file
    data_frame = pd.DataFrame.from_csv(csv_file)

    # Get only entries we care about
    df_bool = (data_frame['Instance type'] == instance_type) & \
              (data_frame['Product'] == product) & \
              (data_frame['Availability zone'] == av_zone)
    df_subset = data_frame[df_bool]

    # Return time series
    return spot_history


# Main routine
def main(proc_time, num_jobs, nodes, jobs_per, in_gb, out_gb,
         bid_price, instance_type, av_zone, product, csv_file=None):
    '''
    Function to calculate spot instance run statistics based on job
    submission parameters; this function will save the statistics and
    specific spot history in csv dataframes to execution directory

    Parameters
    ----------
    proc_time : float
        the number of minutes a single job of interest takes to run
    num_jobs : integer
        total number of jobs to run to complete job submission
    nodes : integer
        the number of slave nodes to run the job submission over;
        the number of nodes is in addition to a master node
    jobs_per : integer
        the number of jobs to run per node
    in_gb : float
        the total amount of input data for a particular job (in GB)
    out_gb : float
        the total amount of output data from a particular job (in GB)
    bid_price : float
        the dollar amount per hour the user is willing to pay for spot
        instance usage on AWS
    instance_type : string
        type of instance to run the jobs on and to get spot history for
    av_zone : string
        the AWS EC2 availability zone (sub-region) to get spot history
        from
    product : string
        the type of operating system product to get spot history for
    csv_file : string (optional), default is None
        the filepath to a csv dataframe to get spot history from;
        if not specified, the function will just get the most recent 90
        days worth of spot price history

    Returns
    -------
    spot_history : pd.DataFrame object
        in addition to saving this as './spot_history.csv' the
        dataframe can also be returned as an object in memory
    stat_df : pd.DataFrame object
        in addition to saving this as './<info>_stats.csv' the
        dataframe can also be returned as an object in memory
    '''

    # Import packages
    import dateutil
    import logging
    import numpy as np
    import os
    import pandas as pd
    from record_spot_price import return_spot_history

    # Init variables
    proc_time *= 60.0
    stat_df_cols = ['Start', 'Exec time', 'Run time', 'Wait time',
                    'Total cost', 'Comp cost', 'Storage cost',
                    'Transfer cost', 'Interrupts']
    stat_df = pd.DataFrame(columns=stat_df_cols)

    # Set up logger
    log_path = os.path.join(os.getcwd(), '%s_%s_%.3f-bid_stats.log' % \
                                         (instance_type, av_zone, bid_price))
    stat_log = setup_logger('stat_log', log_path, logging.INFO)

    # Calculate number of iterations given run configuration
    # Round up and assume that we're waiting for all jobs to finish
    # before terminating nodes
    num_iter = np.ceil(num_jobs/(jobs_per*nodes))
    stat_log.info('With %d jobs, %d nodes, and %d jobs running per node...\n' \
                  'job iterations: %d' % (num_jobs, nodes, jobs_per, num_iter))

    # Get spot price history, if we're getting it from a csv dataframe
    if csv_file:
        # Parse dataframe to form history
        spot_history = spothistory_from_dataframe(csv_file, instance_type,
                                                  product, av_zone)

    # Otherwise, just grab latest 90 days
    else:
        sh_list = return_spot_history(None, instance_type, product, av_zone)

        # Convert history into just timepoints and prices list of tuples
        timestamps = [dateutil.parser.parse(sh.timestamp) for sh in sh_list]
        prices = [sh.price for sh in sh_list]

        # Use pandas timeseries and sort in oldest -> newest
        spot_history = pd.Series(prices, timestamps)
        spot_history = spot_history.sort_index()

    # Get interpolated times per second (forward fill)
    interp_seq = pd.date_range(spot_history.index[0], spot_history.index[-1],
                            freq='S')
    interp_history = spot_history.reindex(interp_seq)
    interp_history = interp_history.fillna(method='ffill')

    # Init simulation time series
    sim_seq = pd.date_range(interp_seq[0], interp_seq[-1], freq='20T')
    sim_series = interp_history[sim_seq]

    # Init loop variables
    sim_idx = 0
    sim_length = len(sim_series)
    beg_time = spot_history.index[0]
    end_time = spot_history.index[-1]
    time_needed = num_iter*(proc_time)

    # Iterate through the interpolated timeseries
    for start_time, start_price in sim_series.iteritems():
        # First see if there's enough time to run jobs
        time_window = (end_time-start_time).total_seconds()
        if time_needed > time_window:
            stat_log.info('Total runtime exceeds time window, ending simulation...')
            break
        if sim_idx == 4238:
            print 'rawdog'
        # Simulate running job and get stats from that start time
        try:
            run_time, wait_time, cost, num_interrupts = \
                    simulate_market(start_time, spot_history, interp_history,
                                    proc_time, num_iter, bid_price)
        except Exception as exc:
            stat_log.info('Could not run full simulation because of:\n%s' % exc)
            continue

        # Get total execution time as sum of running and waiting times
        exec_time = run_time + wait_time
        # Get cumulative cost for running N nodes per iteration
        nodes_cost = cost*nodes

        ### Get computation costs ###
        # Add in master node costs - asssumed to be on-demand, t2.small
        master_on_demand = 0.026
        # Charge for all hours and any left over partial-hours as well
        # as first launch (one hour start cost)
        master_cost = 0.026*np.ceil(exec_time/3600.0) + \
                      master_on_demand
        # Sum master and slave nodes for total computation cost
        comp_cost = master_cost + nodes_cost

        ### Get EBS storage costs ###
        # 30 GB EBS storage for CPAC AMI
        cpac_ami_gb = 30
        secs_per_avg_month = (365/12.0)*24*3600
        # Get GB-months
        master_gb_months = cpac_ami_gb*(exec_time/secs_per_avg_month)
        nodes_gb_months = nodes*cpac_ami_gb*(run_time/secs_per_avg_month)
        # Cost is $0.10 per gb-month
        ebs_cost = 0.10*(master_gb_months + nodes_gb_months)

        ### Data transfer/S3 storage costs ###
        s3_xfer_stor_cost = get_s3_costs(av_zone, in_gb, out_gb)

        ### Total cost ###
        total_cost = comp_cost + ebs_cost + s3_xfer_stor_cost

        # Print stats
        stat_log.info('execution time (minutes): %.3f' % (exec_time/60.0))
        stat_log.info('total cost: $%.3f' % total_cost)
        stat_log.info('number of interrupts: %d' % num_interrupts)
        stat_log.info('wait time (minutes): %.3f' % (wait_time/60.0))

        # Add to output dataframe
        stat_df.loc[sim_idx] = [start_time, exec_time/60.0, run_time/60.0,
                                wait_time/60.0, total_cost, comp_cost,
                                ebs_cost, data_xfer_cost, num_interrupts]

        # Print loop status
        sim_idx += 1
        print_loop_status(sim_idx, sim_length)

    # Write dataframe to disk
    stat_csv = os.path.join(os.getcwd(), '%s_%s_%.3f-bid_stats.csv' % \
                                         (instance_type, av_zone, bid_price))
    stat_df.to_csv(stat_csv)

    # Write spot history to disk
    sh_csv = os.path.join(os.getcwd(), 'spot_history.csv')
    spot_history.to_csv(sh_csv)

    # Give simulation-wide statistics
    spot_history_avg = spot_history.mean()
    bid_ratio = bid_price/spot_history_avg
    interrupt_avg = stat_df['Interrupts'].mean()
    exec_time_avg = stat_df['Exec time'].mean()
    cost_avg = stat_df['Total cost'].mean()

    # Print simulation statistics
    stat_log.info('\n' + 72*'-')
    stat_log.info('Submission of %d job iterations, ' \
                  'each takes %.3f mins to run:' % (num_iter, proc_time/60.0))
    stat_log.info('Average spot history price for %s in %s\n' \
                  'between %s and %s is: $%.3f' % \
                  (instance_type, av_zone, beg_time, end_time, spot_history_avg))
    stat_log.info('Spot bid of $%.3f was %.3fx the average price' % \
                  (bid_price, bid_ratio))
    stat_log.info('Average total execution time (mins): %f' % exec_time_avg)
    stat_log.info('Average total cost: $%.3f' % cost_avg)
    stat_log.info('Average number of interruptions: %.3f' % interrupt_avg)
    stat_log.info(72*'-' + '\n')

    # Plot statistics
    #out_df.boxplot()

    # Return dataframes
    return spot_history, stat_df


# Make executable
if __name__ == '__main__':

    # Import packages
    import argparse

    # Init argparser
    parser = argparse.ArgumentParser(description=__doc__)

    # Required arguments
    parser.add_argument('-t', '--proc_time', nargs=1, required=True,
                        type=float, help='Processing time for one job to complete '\
                             'successfully (in minutes)')
    parser.add_argument('-j', '--num_jobs', nargs=1, required=True, type=int,
                        help='Total number of jobs to run in AWS')
    parser.add_argument('-n', '--nodes', nargs=1, required=True, type=int,
                        help='Number of nodes in cluster to launch')
    parser.add_argument('-per', '--jobs_per', nargs=1, required=True, type=int,
                        help='Number of jobs to run per node')
    parser.add_argument('-ig', '--in_gb', nargs=1, required=True, type=float,
                        help='Input size per job in GB to upload to EC2')
    parser.add_argument('-og', '--out_gb', nargs=1, required=True, type=float,
                        help='Output size per job in GB to download from EC2')
    parser.add_argument('-b', '--bid_price', nargs=1, required=True,
                        type=float, help='Spot bid price')
    parser.add_argument('-i', '--instance_type', nargs=1, required=True,
                        type=str, help='Instance type to run the jobs on')

    # Optional arguments
    parser.add_argument('-z', '--av_zone', nargs=1, required=False, type=str,
                        help='Specify availability zone of interest; ' \
                             'default is \'us-east-1b\'')
    parser.add_argument('-p', '--product', nargs=1, required=False, type=str,
                        help='Specify product of interest; ' \
                             'default is \'Linux/Unix\'')
    parser.add_argument('-c', '--csv_file', nargs=1, required=False, type=str,
                        help='Specify csv dataframe to parse histories')

    # Parse arguments
    args = parser.parse_args()

    # Init variables
    proc_time = args.proc_time[0]
    num_jobs = args.num_jobs[0]
    nodes = args.nodes[0]
    jobs_per = args.jobs_per[0]
    in_gb = args.in_gb[0]
    out_gb = args.out_gb[0]
    bid_price = args.bid_price[0]
    instance_type = args.instance_type[0]

    # Try and init optional arguments
    try:
        av_zone = args.av_zone[0]
    except TypeError as exc:
        av_zone = 'us-east-1b'
        print 'No availability zone argument found, using %s...' % av_zone
    try:
        product = args.product[0]
    except TypeError as exc:
        product = 'Linux/UNIX'
        print 'No product argument found, using %s...' % product
    try:
        csv_file = args.csv_file[0]
    except TypeError as exc:
        csv_file = None
        print 'No csv dataframe specified, only using latest history...'

    # Call main routine
    main(proc_time, num_jobs, nodes, jobs_per, in_gb, out_gb,
         bid_price, instance_type, av_zone, product, csv_file)
