# run_spot_sims.py
#
# Author: Daniel Clark

'''
Script to run AWS simulation for a given configuration file in parallel

Usage:
    python run_spot_sims.py -c <config_file> -n <num_cores>
                            -o <out_dir> -s <spot_csv>
'''

# Build processing list
def build_proc_list(config_file, out_dir, spot_csv):
    '''
    Build a list of spot_price_model.main processes

    Parameters
    ----------
    config_file : string
        filepath to the spot model configuration file
    out_dir : string
        directory to output the results of the simulations
    spot_csv : string
        filepath to the spot history csv file

    Returns
    -------
    proc_list : list
        list of multiprocessing.Process objects that run the simulation
    '''

    # Import packages
    import utils
    import yaml
    from multiprocessing import Process

    # Import local modules
    import record_spot_price
    import spot_price_model

    # Init variables
    proc_list = []
    config_dict = yaml.load(open(config_file, 'r'))

    # Build processing list
    for avz in config_dict['av_zone']:
        for br in config_dict['bid_ratio']:
            for nj in config_dict['num_jobs']:
                proc = Process(target=spot_price_model.main,
                               args=(out_dir, config_dict['proc_time'], nj,
                                     config_dict['jobs_per'],
                                     config_dict['in_gb'],
                                     config_dict['out_gb'],
                                     config_dict['out_gb_dl'],
                                     config_dict['up_rate'],
                                     config_dict['down_rate'], br,
                                     config_dict['instance_type'], avz,
                                     config_dict['product'], spot_csv, None))
                proc_list.append(proc)

    # Return process list
    return proc_list


# Make module executable
if __name__ == '__main__':

    # Import packages
    import argparse

    # Import local modules
    import utils

    # Init argparser
    parser = argparse.ArgumentParser(description=__doc__)

    # Required arguments
    parser.add_argument('-c', '--config_file', nargs=1, required=True,
                        type=str, help='Path to AWS sim configuration file')
    parser.add_argument('-n', '--num_cores', nargs=1, required=True,
                        type=int, help='Number of cores to run at once')
    parser.add_argument('-o', '--out_dir', nargs=1, required=True,
                        type=str, help='Output base directory to store results')
    parser.add_argument('-s', '--spot_csv', nargs=1, required=True,
                        type=str, help='Path to spot history csv')

    # Parse arguments
    args = parser.parse_args()

    # Init variables
    config_file = args.config_file[0]
    num_cores = args.num_cores[0]
    out_dir = args.out_dir[0]
    spot_csv = args.spot_csv[0]

    # Build processing list
    proc_list = build_proc_list(config_file, out_dir, spot_csv)

    # Run jobs in parallel
    utils.run_in_parallel(proc_list, num_cores)
