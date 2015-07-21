# spot_price_model.py
#
# Contributing authors: Daniel Clark, Cameron Craddock, 2015

'''
This module contains functions which return the total duration, cost,
expected failure time, expected wait time, and probability of failure
for a job submission to an AWS EC2 SPOT cluster
'''

# Calculate running time and all other data storage/transfers costs
def calc_full_time_costs(run_time, wait_time, node_cost, first_iter_time,
                         num_jobs, num_nodes, jobs_per, av_zone,
                         in_gb, out_gb, out_gb_dl, up_rate, down_rate):
    '''
    Function to take results from the simulate_market function and
    calculate total costs and runtimes with data transfer and storage
    included

    Parameters
    ----------
    run_time : float
        the total number of seconds all of the nodes were up running
    wait_time : float
        the total number of seconds spent waiting for the spot price
        to come down below bid
    node_cost : float
        the per-node running, or instance, cost
    first_iter_time : float
        the number of seconds the first job iteration took to complete;
        this is used to model when outputs begin downloading from EC2
    num_jobs : integer
        total number of jobs to run to complete job submission
    num_nodes : integer
        the number of nodes that the cluster uses to run job submission
    jobs_per : integer
        the number of jobs to run per node
    av_zone : string
        the AWS EC2 availability zone (sub-region) to get spot history
        from
    in_gb : float
        the total amount of input data for a particular job (in GB)
    out_gb : float
        the total amount of output data from a particular job (in GB)
    out_gb_dl : float
        the total amount of output data to download from EC2 (in GB)
    up_rate : float
        the average upload rate to transfer data to EC2 (in Mb/s)
    down_rate : float
        the average download rate to transfer data from EC2 (in Mb/s)

    Returns
    -------
    total_cost : float
        the total amount of dollars the job submission cost
    instance_cost : float
        cost for running the instances, includes master and all slave
        nodes
    storage_cost : float
        cost associated with data storage
    xfer_cost : float
        cost associated with data transfer (out only as in is free)
    total_time : float
        the total amount of seconds for the entire job submission to
        complete
    run_time : float
        returns the input parameter run_time for convenience
    wait_time : float
        returns the input parameter wait_time for convenience
    xfer_up_time : float
        the amount of time it took to transfer the input data up to
        the master node (seconds)
    xfer_down_time : float
        the amount of time it took to transfer all of the output data
        from the master node (seconds)
    '''

    # Import packages
    import numpy as np

    # Init variables
    cpac_ami_gb = 30
    secs_per_avg_month = (365/12.0)*24*3600
    num_iter = np.ceil(num_jobs/float((jobs_per*num_nodes)))

    # Get total execution time as sum of running and waiting times
    exec_time = run_time + wait_time

    ### Master runtime + storage time (EBS) + xfer time ###
    up_gb_per_sec = up_rate/8.0/1000.0
    down_gb_per_sec = down_rate/8.0/1000.0
    xfer_up_time = num_jobs*(in_gb/up_gb_per_sec)

    # Get the number of jobs ran through n-1 iterations
    num_jobs_n1 = ((num_iter-1)*num_nodes*jobs_per)
    # Calculate how long it takes to transfer down all of the jobs
    # *This is modeled as happening as the jobs finish during the full run
    xfer_down_time_n1 = num_jobs_n1*(out_gb_dl/down_gb_per_sec)
    exec_time_n1 = exec_time - first_iter_time
    residual_jobs = num_jobs - num_jobs_n1

    # End of download of master node
    master_up_time = xfer_up_time + \
                     first_iter_time + \
                     np.max([exec_time_n1, xfer_down_time_n1]) + \
                     residual_jobs*(out_gb_dl/down_gb_per_sec)

    # Get total transfer down time
    xfer_down_time = xfer_down_time_n1 + residual_jobs*(out_gb_dl/down_gb_per_sec)

    ### Get EBS storage costs ###
    ebs_ssd = get_ec2_costs(av_zone, 'ssd')
    ebs_nfs_gb = num_jobs*(in_gb+out_gb)

    # Get GB-months
    master_gb_months = (ebs_nfs_gb+cpac_ami_gb)*\
            (3600.0*np.ceil(master_up_time/3600.0)/secs_per_avg_month)
    nodes_gb_months = num_nodes*cpac_ami_gb*(3600.0*np.ceil(run_time/3600.0)/secs_per_avg_month)
    storage_cost = ebs_ssd*(master_gb_months + nodes_gb_months)

    ### Get computation costs ###
    # Add in master node costs - asssumed to be on-demand, t2.small
    master_on_demand = get_ec2_costs(av_zone, 'master')
    master_cost = master_on_demand*np.ceil(master_up_time/3600.0)
    # Get cumulative cost for running N nodes per iteration
    nodes_cost = node_cost*num_nodes
    # Sum master and slave nodes for total computation cost
    instance_cost = master_cost + nodes_cost

    ### Data transfer costs ###
    ec2_xfer_out = get_ec2_costs(av_zone, 'xfer')
    xfer_cost = ec2_xfer_out*(num_jobs*out_gb_dl)

    ### Total cost ###
    total_cost = instance_cost + storage_cost + xfer_cost
    ### Total time ###
    total_time = master_up_time

    # Return data frame entries
    return total_cost, instance_cost, storage_cost, xfer_cost, \
           total_time, run_time, wait_time, \
           xfer_up_time, xfer_down_time


# Calculate cost over interval
def calculate_cost(start_time, uptime_seconds, interp_history, interrupted=False):
    '''
    Function to calculate the runtime spot cost associated with an
    instance's spot history

    Parameters
    ----------
    start_time : datetime.datetime object
        start time of the spot history to calculate from
    uptime_seconds : float
        the number of seconds that the instance was running for
    interp_history : pandas.Series object
        the interpolated (second-resolution) spot history series,
        where the index is a timestamp and the values are prices
    interrupted : boolean (optional), default=False
        indicator of whether the instance was interrupted before
        terminating or not

    Returns
    -------
    total_cost : float
        the total amount of $ that the instance cost
    '''

    # Import packages
    import numpy as np
    import pandas as pd
    import datetime

    # Init variables
    pay_periods = np.ceil(uptime_seconds/3600.0)
    end_time = start_time + datetime.timedelta(seconds=uptime_seconds)
    hour_seq = pd.date_range(start_time, periods=pay_periods, freq='H')
    hourly_series = interp_history[hour_seq]

    # Sum up all but last hour price if interrupted
    total_cost = hourly_series[:-1].sum()

    # If the user ran residual time without interrupt after last hour
    if not interrupted:
        total_cost += hourly_series[-1]

    # Return the total cost
    return total_cost


# Lookup tables for pricing for EBS
def get_ec2_costs(av_zone, cost_type):
    '''
    Function to retrieve costs associated with using EC2

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
    EC2 pricing: http://aws.amazon.com/ec2/pricing/
    EBS pricing: http://aws.amazon.com/ebs/pricing/

    Parameters
    ----------
    av_zone : string
        the availability zone to get the pricing info for
    cost_type : string
        the type of cost to extract, supported types include:
        'ssd' - ssd EC2 EBS storage
        'mag' - magnetic EC2 EBS storage
        'xfer' - download from EC2 transfer costs
        'master' - t2.small hourly on-demand cost

    Returns
    -------
    ec2_cost : float
        the $ amount per unit of the cost type of interest
    '''

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

    # Get $/hour costs of running t2.small master node
    ec2_t2_small = {'us-east-1' : 0.026,
                    'us-west-1' : 0.034,
                    'us-west-2' : 0.026,
                    'eu-west-1' : 0.028,
                    'eu-central-1' : 0.030,
                    'ap-southeast-1' : 0.040,
                    'ap-southeast-2' : 0.040,
                    'ap-northeast-1' : 0.040,
                    'sa-east-1' : 0.054}

    # Select costs type
    if cost_type == 'ssd':
        ec2_cost = ebs_gen_purp[region]
    elif cost_type == 'mag':
        ec2_cost = ebs_mag[region]
    elif cost_type == 'xfer':
        ec2_cost = ec2_xfer_out[region]
    elif cost_type == 'master':
        ec2_cost = ec2_t2_small[region]
    else:
        err_msg = 'cost_type argument does not support %s' % cost_type
        raise Exception(err_msg)

    # Return the ec2 cost
    return ec2_cost


# Lookup tables for pricing for EBS
def get_s3_costs(av_zone, in_gb, out_gb, num_jobs):
    '''
    Data transfer to S3 from anywhere is free (all regions)
    Data transfer from S3 to EC2 in same region is free (all regions)

    Parameters
    ----------
    av_zone : string
        the availability zone to get the pricing info for
    in_gb : float
        the amount of gigabytes to upload to S3
    out_gb : float
        the amount of gigabytes to upload to S3
    num_jobs : integer
        the number of jobs that will be used to gauge how many S3
        requests that are made to return an accurate price

    Returns
    -------
    s3_price : float
        the $ amount per unit of the cost type of interest

    References
    ----------
    S3 pricing: http://aws.amazon.com/s3/pricing/
    '''

    # Init variables
    region = av_zone[:-1]

    # How many input/output files get generated per job
    # Assume ~2 for input
    in_ratio = 2
    # Assume ~50 for outupt
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
        the per-node running, or instance, cost
    num_interrupts : integer
        the number of times the job submission was interrupted
    first_iter_time : float
        the number of seconds the first job iteration took to complete;
        this is used to model when outputs begin downloading from EC2
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
    start_idx = np.argmax(spot_history.index >= start_time)
    spot_history_start = spot_history.index[start_idx]

    # Init remaining rumtime
    remaining_runtime = proc_time*num_iter

    # Init 1st iteration time
    first_iter_flg = False
    first_iter_time = 0

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
        # Otherwise, start instances
        else:
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

            # Get the next time we can start
            spot_history_start = min(start_times)
            # and set as the next spot time
            start_time = spot_history_start

            # And increment wait time by (next start)-(this interrupt)
            total_wait += (start_time - interrupt_time).total_seconds()

        # Check to see if we're setting first iter
        if not first_iter_flg:
            # If we were up for at least one amount of processing time
            if total_runtime >= proc_time:
                first_iter_time = proc_time + total_wait
                first_iter_flag = True

    # Return results
    return total_runtime, total_wait, total_cost, num_interrupts, first_iter_time


# Return a time series from csv data frame
def spothistory_from_dataframe(csv_file, instance_type, product, av_zone):
    '''
    Function to return a time and price series from a csv dataframe

    Parameters
    ----------
    csv_file : string
        file path to dataframe csv file
    instance_type : string
        the type of instance to gather spot history for
    product : string
        the type of OS product to gather spot history for
    av_zone : string
        the availability zone to get the pricing info for

    Returns
    -------
    spot_history : pandas.Series
        time series of spot history prices indexed by timestamp
    '''

    # Import packages
    import dateutil.parser
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

    # Get spot histories from data frame with str timestamps
    spot_history = df_subset.set_index('Timestamp')['Price']
    spot_history = spot_history.sort_index()

    # Get new histories with datetime timestamps
    datetimes = [dateutil.parser.parse(ts) for ts in spot_history.index]
    spot_history = pd.Series(spot_history.values, datetimes)

    # Return time series
    return spot_history


# Main routine
def main(sim_dir, proc_time, num_jobs, jobs_per, in_gb, out_gb, out_gb_dl,
         up_rate, down_rate, bid_ratio, instance_type, av_zone, product,
         csv_file=None, toy_data=None):
    '''
    Function to calculate spot instance run statistics based on job
    submission parameters; this function will save the statistics and
    specific spot history in csv dataframes to execution directory

    Parameters
    ----------
    sim_dir : string
        base directory where to create the availability zone folders
        for storing the simulation results
    proc_time : float
        the number of minutes a single job of interest takes to run
    num_jobs : integer
        total number of jobs to run to complete job submission
    jobs_per : integer
        the number of jobs to run per node
    in_gb : float
        the total amount of input data for a particular job (in GB)
    out_gb : float
        the total amount of output data from a particular job (in GB)
    out_gb_dl : float
        the total amount of output data to download from EC2 (in GB)
    up_rate : float
        the average upload rate to transfer data to EC2 (in Mb/s)
    down_rate : float
        the average download rate to transfer data from EC2 (in Mb/s)
    bid_ratio : float
        the ratio to average spot history price to set the bid price to
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
    toy_data : boolean
        flag indicating whether to only run the simulation once
        since the data is the same across time anyway

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
    import yaml

    # Import local packages
    import utils
    from record_spot_price import return_spot_history

    # Init variables
    proc_time *= 60.0
    num_nodes = min(np.ceil(float(num_jobs)/jobs_per), 20)

    # Init simulation market results dataframe
    sim_df_cols = ['start_time', 'spot_hist_csv', 'proc_time', 'num_datasets',
                   'jobs_per_node', 'num_jobs_iter', 'bid_ratio', 'bid_price',
                   'median_history', 'mean_history', 'stdev_history',
                   'compute_time', 'wait_time', 'per_node_cost',
                   'num_interrupts', 'first_iter_time']
    sim_df = pd.DataFrame(columns=sim_df_cols)

    # Init full run stats data frame
    stat_df_cols = ['Total cost', 'Instance cost', 'Storage cost', 'Tranfer cost',
                    'Total time', 'Run time', 'Wait time',
                    'Upload time', 'Download time']
    stat_df = pd.DataFrame(columns=stat_df_cols)

    # Set up logger
    base_dir = os.path.join(sim_dir, av_zone)
    if not os.path.exists(base_dir):
        try:
            os.makedirs(base_dir)
        except OSError as exc:
            print 'Found av zone directory %s, continuing...' % av_zone
    log_path = os.path.join(base_dir, '%s_%d-jobs_%.3f-bid.log' % \
                            (instance_type, num_jobs, bid_ratio))
    stat_log = utils.setup_logger('stat_log', log_path, logging.INFO, to_screen=True)

    # Check to see if simulation was already run (sim csv file exists)
    sim_csv = os.path.join(base_dir, '%s_%d-jobs_%.3f-bid_sim.csv' % \
                           (instance_type, num_jobs, bid_ratio))
    if os.path.exists(sim_csv):
        stat_log.info('Simulation file %s already exits, skipping...' % sim_csv)
        return

    # Calculate number of iterations given run configuration
    # Round up and assume that we're waiting for all jobs to finish
    # before terminating nodes
    num_iter = np.ceil(num_jobs/float((jobs_per*num_nodes)))
    stat_log.info('With %d jobs, %d nodes, and %d jobs running per node...\n' \
                  'job iterations: %d' % (num_jobs, num_nodes, jobs_per, num_iter))

    # Get spot price history, if we're getting it from a csv dataframe
    if csv_file:
        # Parse dataframe to form history
        spot_history = spothistory_from_dataframe(csv_file, instance_type,
                                                  product, av_zone)
        # Get rid of any duplicated timestamps
        spot_history = spot_history.groupby(spot_history.index).first()

    # Otherwise, just grab latest 90 days
    else:
        sh_list = return_spot_history(None, instance_type, product, av_zone)

        # Convert history into just timepoints and prices list of tuples
        timestamps = [dateutil.parser.parse(sh.timestamp) for sh in sh_list]
        prices = [sh.price for sh in sh_list]

        # Use pandas timeseries and sort in oldest -> newest
        spot_history = pd.Series(prices, timestamps)
        spot_history = spot_history.sort_index()

        # Write spot history to disk
        sh_csv = os.path.join(os.getcwd(), 'spot_history.csv')
        spot_history.to_csv(sh_csv)

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

    # Get bid price
    spot_history_avg = spot_history.mean()
    bid_price = bid_ratio*spot_history_avg
    stat_log.info('Spot history average is $%.3f, bid ratio of %.3fx sets ' \
                  'bid to $%.3f' % (spot_history_avg, bid_ratio, bid_price))

    # Iterate through the interpolated timeseries
    for start_time, start_price in sim_series.iteritems():
        if not toy_data:
            # First see if there's enough time to run jobs
            time_window = (end_time-start_time).total_seconds()
            if time_needed > time_window:
                stat_log.info('Total runtime exceeds time window, ending simulation...')

            # Simulate running job and get stats from that start time
            try:
                run_time, wait_time, pernode_cost, num_interrupts, first_iter_time = \
                        simulate_market(start_time, spot_history, interp_history,
                                        proc_time, num_iter, bid_price)
            except Exception as exc:
                stat_log.info('Could not run full simulation because of:\n%s' % exc)
                continue
        else:
            run_time = num_iter*proc_time
            wait_time = 0
            pernode_cost = np.ceil(run_time/3600.0)*interp_history[1]
            num_interrupts = 0
            first_iter_time = proc_time

        # Write simulate market output to dataframe
        sim_df.loc[sim_idx] = [start_time, csv_file, proc_time, num_jobs,
                               jobs_per, num_iter, bid_ratio, bid_price,
                               np.mean(spot_history), np.median(spot_history),
                               np.std(spot_history), run_time, wait_time,
                               pernode_cost, num_interrupts, first_iter_time]

        # Get complete time and costs from spot market simulation paramters
        total_cost, instance_cost, stor_cost, xfer_cost, \
        total_time, run_time, wait_time, \
        xfer_up_time, xfer_down_time = \
                calc_full_time_costs(run_time, wait_time, pernode_cost, first_iter_time,
                                     num_jobs, num_nodes, jobs_per, av_zone,
                                     in_gb, out_gb, out_gb_dl, up_rate, down_rate)

        # Add to output dataframe
        stat_df.loc[sim_idx] = [total_cost, instance_cost, stor_cost, xfer_cost,
                                total_time/60.0, run_time/60.0, wait_time/60.0,
                                xfer_up_time/60.0, xfer_down_time/60.0]

        # Print stats
        stat_log.info('Total cost: $%.3f' % total_cost)
        stat_log.info('Total time (minutes): %.3f' % (total_time/60.0))
        stat_log.info('run time (minutes): %.3f' % (run_time/60.0))
        stat_log.info('per-node cost: $%.3f' % pernode_cost)
        stat_log.info('number of interrupts: %d' % num_interrupts)
        stat_log.info('wait time (minutes): %.3f' % (wait_time/60.0))

        # Print loop status
        if toy_data:
            break
        else:
            sim_idx += 1
            utils.print_loop_status(sim_idx, sim_length)

    # Write simulation dataframe to disk
    sim_df.to_csv(sim_csv)

    # Write stats dataframe to disk
    stat_csv = os.path.join(base_dir, '%s_%d-jobs_%.3f-bid_stats.csv' % \
                           (instance_type, num_jobs, bid_ratio))
    stat_df.to_csv(stat_csv)

    # Write parameters yaml to disk
    params_yml = os.path.join(base_dir, '%s_%d-jobs_%.3f-bid_params.yml' % \
                              (instance_type, num_jobs, bid_ratio))

    params = {'proc_time' : proc_time,
              'num_jobs' : num_jobs,
              'jobs_per' : jobs_per,
              'in_gb' : in_gb,
              'out_gb' : out_gb,
              'out_gb_dl' : out_gb_dl,
              'up_rate' : up_rate,
              'down_rate' : down_rate,
              'bid_ratio' : bid_ratio,
              'instance_type' : instance_type,
              'av_zone' : av_zone,
              'product' : product,
              'csv_file' : csv_file}

    with open(params_yml, 'w') as y_file:
        y_file.write(yaml.dump(params))

    # Give simulation-wide statistics
    interrupt_avg = sim_df['Interrupts'].mean()
    time_avg = stat_df['Total time'].mean()
    cost_avg = stat_df['Total cost'].mean()

    # Print simulation statistics
    stat_log.info('\n' + 72*'-')
    stat_log.info('Submission of %d job iterations, ' \
                  'each takes %.3f mins to run:' % (num_iter, proc_time/60.0))
    stat_log.info('Average spot history price for %s in %s\n' \
                  'between %s and %s is: $%.3f' % \
                  (instance_type, av_zone, beg_time, end_time, spot_history_avg))
    stat_log.info('Spot ratio of %.3fx the average price set bid to $%.3f' % \
                  (bid_ratio, bid_price))
    stat_log.info('Average total time (mins): %f' % time_avg)
    stat_log.info('Average total cost: $%.3f' % cost_avg)
    stat_log.info('Average number of interruptions: %.3f' % interrupt_avg)
    stat_log.info(72*'-' + '\n')

    # Return dataframes
    return spot_history, sim_df, stat_df


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
    parser.add_argument('-per', '--jobs_per', nargs=1, required=True, type=int,
                        help='Number of jobs to run per node')
    parser.add_argument('-ig', '--in_gb', nargs=1, required=True, type=float,
                        help='Input size per job in GB to upload to EC2')
    parser.add_argument('-og', '--out_gb', nargs=1, required=True, type=float,
                        help='Output size per job in GB to store in EC2 EBS')
    parser.add_argument('-od', '--out_gb_dl', nargs=1, required=True, type=float,
                        help='Output size per job in GB to download from EC2')
    parser.add_argument('-ur', '--up_rate', nargs=1, required=True, type=float,
                        help='Upload rate in Mb/sec')
    parser.add_argument('-dr', '--down_rate', nargs=1, required=True, type=float,
                        help='Download rate in Mb/sec')
    parser.add_argument('-b', '--bid_ratio', nargs=1, required=True,
                        type=float, help='Bid ratio to average spot price')
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
    parser.add_argument('-td', '--toy_data', required=False, action='store_true',
                        help='Specify if the data is toy (fixed price) or not')

    # Parse arguments
    args = parser.parse_args()

    # Init variables
    # Pipeline config params
    proc_time = args.proc_time[0]
    num_jobs = args.num_jobs[0]
    # Cluster config params
    jobs_per = args.jobs_per[0]
    instance_type = args.instance_type[0]
    # Data in/out to store EBS
    in_gb = args.in_gb[0]
    out_gb = args.out_gb[0]
    # Data transfer
    out_gb_dl = args.out_gb_dl[0]
    up_rate = args.up_rate[0]
    down_rate = args.down_rate[0]
    # Bid ratio
    bid_ratio = args.bid_ratio[0]

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
    try:
        toy_data = args.toy_data
    except TypeError as exc:
        toy_data = None
        print 'Not toy data, assuming real spot history data...'

    # Call main routine
    main(proc_time, num_jobs, jobs_per, in_gb, out_gb, out_gb_dl,
         up_rate, down_rate, bid_ratio, instance_type, av_zone, product,
         csv_file, toy_data)
