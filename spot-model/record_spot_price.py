# record_spot_price.py
#
# Author: Daniel Clark, 2015

'''
This module records the spot price from AWS EC2 continuously and saves
the information to dataframes as a csv files to an output directory

Usage:
    python record_spot_price.py -o <output_base_directory>
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


# Return the spot history dataframe for certain categories
def return_sh_df(start_time, instance_type, product, av_zone):
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
                    str(sh_record.region.name),
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


# Main routine
def main(out_dir):
    '''
    Function to fetch the latest spot history from AWS and store in a
    dataframe saved to a local csv file for every availability zone

    Parameters
    ----------
    out_dir : string
        base file directory to store the spot history dataframes
    '''

    # Import packages
    import boto
    import datetime
    import logging
    import os
    import pandas as pd

    # Import local packages
    import utils

    # Set up logger
    now_date = datetime.datetime.now()
    log_month = now_date.strftime('%m-%Y')
    log_path = os.path.join(out_dir, 'spot_history_'+log_month+'.log')

    sh_log = utils.setup_logger('sh_log', log_path, logging.INFO, to_screen=True)

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
    reg_len = len(regions)
    ip_len = len(instance_products)

    # For each AWS region
    for reg_idx, region in enumerate(regions):
        # Get the availability zones
        av_zones = return_av_zones(region)
        av_len = len(av_zones)
        # For each availability zone
        for av_idx, av_zone in enumerate(av_zones):
            # For each instance_type-product combination
            for ip_idx, (instance_type, product) in enumerate(instance_products):
                # Grab the spot history
                df = return_sh_df(None, instance_type, product, av_zone)

                # Create csv path
                out_csv = os.path.join(out_dir, log_month, str(region.name),
                                       product.replace('/', '-'),
                                       instance_type + '.csv')

                # Check to see if folder needs to be created
                csv_dir = os.path.dirname(out_csv)
                if not os.path.exists(csv_dir):
                    os.makedirs(csv_dir)

                # Save the dataframe
                df.to_csv(out_csv)

                # Log instance type finished
                sh_log.info('%d/%d instance-products completed' % (ip_idx+1, ip_len))

            # Log instance type finished
            sh_log.info('%d/%d availability zones completed' % (av_idx+1, av_len))

        # Print region complete
        sh_log.info('%d/%d regions completed' % (reg_idx+1, reg_len))


# Make script executable
if __name__ == '__main__':

    # Import packages
    import argparse

    # Init argparser
    parser = argparse.ArgumentParser(description=__doc__)

    # Required arguments
    parser.add_argument('-o', '--out_dir', nargs=1, required=True,
                        type=str, help='Base directory to store spot '\
                        'history data frames')

    # Parse arguments
    args = parser.parse_args()

    # Init variables
    # Pipeline config params
    out_dir = args.out_dir[0]

    # Run main routine
    main(out_dir)
