# record_spot_price.py
#
# Author: Daniel Clark, 2015

'''
This module records the spot price from AWS EC2 continuously and saves
the information to a dataframe as a csv file; if a csv file is not
provided, one will be initialized in the current directory

Usage:
    python record_spot_price.py [-c <csv_file>]
'''

# Initialize categorical variables for spot price history
def init_categories():
    '''
    Function that initializes and returns ec2 instance categories

    Parameters
    ----------
    None

    Returns
    -------
    instance_types : list
        a list of strings containing the various types of instances
    product_descriptions : list
        a list of strings containing the different ec2 OS products
    '''

    # Init variables
    instance_types = ['t2.micro', 't2.small', 't2.medium',
                      'm3.medium', 'm3.large', 'm3.xlarge', 'm3.2xlarge',
                      'c4.large', 'c4.xlarge', 'c4.2xlarge', 'c4.4xlarge',
                      'c4.8xlarge', 'c3.large', 'c3.xlarge', 'c3.2xlarge',
                      'c3.4xlarge', 'c3.8xlarge', 'r3.large', 'r3.xlarge',
                      'r3.2xlarge', 'r3.4xlarge', 'r3.8xlarge',
                      'i2.xlarge', 'i2.2xlarge', 'i2.4xlarge', 'i2.8xlarge',
                      'd2.xlarge', 'd2.2xlarge', 'd2.4xlarge', 'd2.8xlarge',
                      'g2.2xlarge', 'g2.8xlarge']
    product_descriptions = ['Linux/UNIX', 'SUSE Linux',
                            'Linux/UNIX (Amazon VPC)', 'SUSE Linux (Amazon VPC)']

    # Return variables
    return instance_types, product_descriptions


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


# Return the spot history dataframe for certain categories
def return_sh_df(start_time, instance_type, product, region, av_zone):
    '''
    Function to return the spot prices and timestamps
    '''

    # Import packages
    import pandas as pd

    # Init variables
    df_cols = ['Instance type', 'Product', 'Region', 'Availability zone',
               'Spot price', 'Timestamp']
    new_df = pd.DataFrame(columns=df_cols)

    # Get spot history
    sh_list = return_spot_history(start_time, instance_type, product, av_zone)

    # Populate new dataframe
    for idx, sh_record in enumerate(sh_list):
        df_entry = [str(sh_record.instance_type),
                    str(sh_record.product_description),
                    str(sh_record.region),
                    str(sh_record.availability_zone),
                    sh_record.price, str(sh_record.timestamp)]
        new_df.loc[idx] = df_entry

    # Return new dataframe
    return new_df


# Return a list of spot price histories
def return_spot_history(start_time, instance_type, product, av_zone):
    '''
    Function to return a list of SpotPriceHistory objects

    Parameters
    ----------
    start_time : string
        the start time of interest to begin collecting histories
    instance_type : string
        the type of interest to collect the histories for
    product : string
        the OS product platform to collect histories for
    av_zone : string
        the availability zone to collect histories for

    Returns
    -------
    full_sh_list : list
        a list of boto.ec2.spotpricechistory.SpotPriceHistory objects;
        each object contains price, timestamp, and other information
    '''

    # Import packages
    import boto
    import boto.ec2
    import logging

    # Init variables
    full_sh_list = []
    regions = boto.ec2.regions()

    # Grab region of interest and connect to ec2 in that region
    reg_name = av_zone[:-1]
    region = [reg for reg in regions if reg_name in reg.name][0]
    ec2_conn = boto.connect_ec2(region=region)

    # Get logger
    sh_log = logging.getLogger('sh_log')

    # While the token flag indicates to more data
    token_flg = True
    next_token = None
    while token_flg:
        # Grab batch of histories
        sh_list = \
                ec2_conn.get_spot_price_history(start_time=start_time,
                                                instance_type=instance_type,
                                                product_description=product,
                                                availability_zone=av_zone,
                                                next_token=next_token)
        # Grab next token for next batch of histories
        next_token = sh_list.nextToken

        # Update list if it has elements and log
        if len(sh_list) > 0:
            first_ts = str(sh_list[0].timestamp)
            last_ts = str(sh_list[-1].timestamp)
            sh_log.info('Appending to list: %s - %s' % (first_ts, last_ts))
            full_sh_list.extend(sh_list)
        else:
            sh_log.info('Found no spot history in %s, moving on...' % av_zone)

        # Check if list is still returning 1000 objects
        if len(sh_list) != 1000:
            token_flg = False

    # Return full spot history list
    return full_sh_list


# Get availability zones
def return_av_zones(region):
    '''
    Function to get a list of the availability zones as strings

    Parameters
    ----------
    region : boto.regioninfo.RegionInfo object
        the region object to get the zones from

    Returns
    -------
    av_zones : list
        a list of strings of the availability zone names
    '''

    # Import packages
    import boto
    import boto.ec2

    # Init variables
    ec2_conn = boto.connect_ec2(region=region)
    av_zones = ec2_conn.get_all_zones()

    # Get names as strings
    av_zones = [str(av_zone.name) for av_zone in av_zones]

    # Return list of availability zones
    return av_zones


# Get all of the data from the past 90 days
def just_fetch_everything(instance_type, product, region, out_dir):
    '''
    Function which iterates through the availability zones of a region
    and pulls down all of the available spot history it can to archive

    Parameters
    ----------
    instance_type : string
        the type of instance to get the history for (e.g. 'c3.8xlarge')
    product : string
        the type of platform for the instance (e.g. 'Linux/UNIX')
    region : boto.regioninfo.ReionInfo object
        an object that describes the region to pull history frun
    out_dir : string
        filepath to the base output directory to log data to

    Returns
    -------
    None
        this function does not return any data or datatype
    '''

    # Import packages
    import boto
    import datetime
    import gzip
    import logging
    import os
    import pickle

    # Init variables
    av_zones = return_av_zones(region)
    full_sh_list = []

    # Get date
    utc_now = datetime.datetime.utcnow()
    utc_date = utc_now.strftime('%m-%d-%Y')

    # Form output path for spot histories
    file_path = os.path.join(out_dir, utc_date, str(region.name),
                             product.replace('/', '-'), instance_type+'.pklz')
    # Get logger
    sh_log = logging.getLogger('sh_log')

    # For each availability zone, get spot histories and append to one list
    for av_zone in av_zones:
        av_list = return_spot_history(None, instance_type, product, av_zone)
        full_sh_list.extend(av_list)

    # Pickle the list and save as compressed pklz
    sh_log.info('Saving entire spot history to disk as %s...' % file_path)
    file_dir = os.path.dirname(file_path)
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)
    with gzip.open(file_path, 'wb') as file_out:
        pickle.dump(full_sh_list, file_out)


# Return the previous end time of the last spot history
def return_prev_end_time(spot_hist_df, instance_type, product, region, av_zone):
    '''
    '''

    # Import packages
    import datetime
    import dateutil
    import logging
    import pytz
    import sys

    # Init variables
    utc_now = datetime.datetime.utcnow()
    utc_now = utc_now.replace(tzinfo=pytz.utc)

    # Get only relevant data
    df_mask = (spot_hist_df['Instance type'] == instance_type) & \
              (spot_hist_df['Product'] == product) & \
              (spot_hist_df['Region'] == str(region.name)) & \
              (spot_hist_df['Availability zone'] == av_zone)

    rel_df = spot_hist_df[df_mask]

    # Iterate through list and find most recent timestamp
    sh_log = logging.getLogger('sh_log')
    sh_log.info('Searching for most recent timestamp...')

    # Init loop variables
    min_diff = sys.maxint
    min_ts = None

    # For each string time stamp in dataframe, find difference from now
    for idx, ts in enumerate(rel_df['Timestamp']):
        # Get then as formatted datetime obj
        utc_then = dateutil.parser.parse(ts)

        # Find difference between then and now
        diff_time = utc_now - utc_then
        diff_secs = diff_time.total_seconds()

        # Test if its smaller than the minimum difference
        if diff_secs < min_diff:
            min_diff = diff_secs
            min_ts = ts

    # Return end time
    return min_ts


# Main routine
def main(csv_file):
    '''
    Function to fetch the latest spot history from AWS and store in a
    dataframe saved to a local csv file

    Parameters
    ----------
    csv_file : string
        filepath to local csv file to save the data frame to
    '''

    # Import packages
    import boto
    import datetime
    import logging
    import os
    import pandas as pd

    # If csv file wasn't specified, create new df and write to disk
    if csv_file is None:
        init_df = True
        csv_file = os.path.join(os.getcwd(), 'spot_histories.csv')
        df_cols = ['Instance type', 'Product', 'Region', 'Availability zone',
                   'Spot price', 'Timestamp']
        # Create df and write to csv
        spot_hist_df = pd.DataFrame(columns=df_cols)
        spot_hist_df.to_csv(csv_file)
    # Otherwise, just use csv file as dataframe
    else:
        init_df = False
        spot_hist_df = pd.DataFrame.from_csv(csv_file)

    # Get folder where csv_file is
    out_dir = os.path.dirname(csv_file)

    # Set up logger
    now_date = datetime.datetime.now()
    log_month = now_date.strftime('%m-%Y')
    log_path = os.path.join(out_dir, 'spot_history_'+log_month+'.log')

    sh_log = setup_logger('sh_log', log_path, logging.INFO, to_screen=True)

    # Get list of regions
    reg_conn = boto.connect_ec2()
    regions = reg_conn.get_all_regions()
    reg_conn.close()

    # Init categories to iterate through
    instance_types, product_descriptions = init_categories()

    # Form a list of the combinations of instance types and products
    instance_products = [(inst_type, prod) for inst_type in instance_types \
                                           for prod in product_descriptions]

    # Get total lengths
    reg_tot = len(regions)
    ip_tot = len(instance_products)

    # For each AWS region
    for reg_idx, region in enumerate(regions):
        # For each instance_type-product combination
        for ip_idx, (instance_type, product) in enumerate(instance_products):
            # First just save everything
            just_fetch_everything(instance_type, product, region, out_dir)

            # Next, get only latest data
            sh_log.info('Acquiring latest spot history for (%s, %s, %s)...' \
                  % (instance_type, product, region))

            # Get avail zones
            av_zones = return_av_zones(region)

            # Iterate through availability zones
            for av_zone in av_zones:
                # Get last entry's end time
                if init_df:
                    prev_end_time = None
                else:
                    prev_end_time = \
                            return_prev_end_time(spot_hist_df, instance_type,
                                                 product, region, av_zone)
                # Get the latest spot prices and time stamps
                new_df = return_sh_df(prev_end_time, instance_type, product,
                                      region, av_zone)
                # Append to csv
                new_df.to_csv(csv_file, header=False, mode='a')

                # Print done
                print 'Added spot history to csv: %s' % csv_file

            # Log instance type finished
            sh_log.info('%d/%d instance-products completed' % (ip_idx+1, ip_tot))

        # Print region complete
        sh_log.info('%d/%d regions completed' % (reg_idx+1, reg_tot))


# Make script executable
if __name__ == '__main__':

    # Import packages
    import argparse
    import os

    # Init argparser
    parser = argparse.ArgumentParser(description=__doc__)

    # Optional arguments
    parser.add_argument('-c', '--csv_file', nargs=1, required=False,
                        help='Filepath to data frame csv file')

    # Parse arguments
    args = parser.parse_args()

    # Init variables
    try:
        csv_file = args.csv_file[0]
    except TypeError as exc:
        csv_file = None

    # Run main routine
    main(csv_file)
