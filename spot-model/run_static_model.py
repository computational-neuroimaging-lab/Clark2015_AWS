# run_static_model.py
#
# Author: Daniel Clark, 2015

'''
'''

# Load config file and run static model
def load_and_run(config, av_zone, price_hr):
    '''
    '''

    # Import packages
    import numpy as np
    import pandas as pd
    import yaml

    # Relative imports
    from spot_price_model import calc_s3_model_costs
    import utils

    # Init variables
    df_rows = []
    cfg_dict = yaml.load(open(config, 'r'))

    # Model parameters
    down_rate = cfg_dict['down_rate']
    in_gb = cfg_dict['in_gb']
    instance_type = cfg_dict['instance_type']
    jobs_per = cfg_dict['jobs_per']
    num_jobs_arr = cfg_dict['num_jobs']
    out_gb = cfg_dict['out_gb']
    out_gb_dl = cfg_dict['out_gb_dl']
    proc_time = cfg_dict['proc_time']
    proc_time *= 60.0 # convert to seconds
    product = cfg_dict['product']
    up_rate = cfg_dict['up_rate']

    # Evaluate for each dataset size (number of jobs)
    for num_jobs in num_jobs_arr:
        print '%d datasets...' % num_jobs

        # Tune parameters for cost model
        num_nodes = min(np.ceil(float(num_jobs)/jobs_per), 20)
        num_iter = np.ceil(num_jobs/float((jobs_per*num_nodes)))

        # Runtime parameters
        run_time = num_iter*proc_time
        wait_time = 0
        pernode_cost = np.ceil(run_time/3600.0)*price_hr
        num_interruipts = 0
        first_iter_time = proc_time

        # Grab costs from s3 model
        print av_zone
        total_cost, instance_cost, ebs_storage_cost, s3_cost, \
        s3_storage_cost, s3_req_cost, s3_xfer_cost, \
        total_time, run_time, wait_time, \
        xfer_up_time, s3_upl_time, s3_download_time = \
            calc_s3_model_costs(run_time, wait_time, pernode_cost,
                                first_iter_time, num_jobs, num_nodes, jobs_per,
                                av_zone, in_gb, out_gb, up_rate, down_rate)

        # Populate dictionary
        row_dict = {'av_zone' : av_zone, 'down_rate' : down_rate,
                    'in_gb' : in_gb, 'instance_type' : instance_type,
                    'jobs_per' : jobs_per, 'num_datasets' : num_jobs,
                    'out_gb' : out_gb, 'out_gb_dl' : out_gb_dl,
                    'proc_time' : proc_time, 'product' : product,
                    'up_rate' : up_rate, 'price_hr' : price_hr,
                    'static_total_cost' : total_cost,
                    'static_instance_cost' : instance_cost,
                    'static_ebs_storage_cost' : ebs_storage_cost,
                    's3_total_cost' : s3_cost,
                    's3_storage_cost' : s3_storage_cost,
                    's3_req_cost' : s3_req_cost,
                    's3_xfer_cost' : s3_xfer_cost,
                    'static_total_time' : total_time,
                    'static_run_time' : run_time,
                    'static_wait_time' : wait_time,
                    'xfer_up_time' : xfer_up_time,
                    's3_upl_time' : s3_upl_time,
                    's3_dl_time' : s3_download_time}

        # Convert to pandas series and add to list
        row_series = pd.Series(row_dict)
        df_rows.append(row_series)

    # Create static model dataframe
    static_df = pd.DataFrame.from_records(df_rows)

    # Return dataframe
    return static_df


# Make executable
if __name__ == '__main__':

    # Import packages
    import argparse

    # Init argparser
    parser = argparse.ArgumentParser(description=__doc__)

    # Required arguments
    parser.add_argument('-c', '--config', nargs=1, required=True,
                        type=str, help='Filepath to the sim config file')
    parser.add_argument('-p', '--price_hr', nargs=1, required=True,
                        type=float, help='Price per compute hour to assume')
    parser.add_argument('-z', '--av_zone', nargs=1, required=False, type=str,
                        help='Specify availability zone of interest')

    # Parse arguments
    args = parser.parse_args()

    # Init variables
    config = args.config[0]
    price_hr = args.price_hr[0]
    av_zone = args.av_zone[0]

    # Call static model function
    static_df = load_and_run(config, av_zone, price_hr)

    # Write to disk
    static_df.to_csv('./static_df.csv')
