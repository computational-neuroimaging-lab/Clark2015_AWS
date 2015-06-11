
def build_big_df(av_zone, zone_dict):
    '''
    '''

    # Import packages
    from spot_price_model import spothistory_from_dataframe
    import pandas as pd

    # Init variables
    df_cols = ['sim_idx', 'av_zone', 'bid_ratio', 'bid_price', 'num_ds', 'sim_start_time', 'num_interrupts', 'first_itr_time', 'total_cost', 'instance_cost', 'stor_cost', 'xfer_cost', 'total_time', 'run_time', 'wait_time', 'xfer_up_time', 'xfer_down_time']
    big_df = pd.DataFrame(columns=df_cols)

    # Print av zone of interest being created
    print av_zone
    spot_history = spothistory_from_dataframe('spot_history/merged_dfs.csv', 'c3.8xlarge', 'Linux/UNIX', av_zone)

    # Iterate through the bid-subs pairs and stat/sim dataframes
    loc = 0
    len_dict = len(zone_dict)
    for idx, (kk, vv) in enumerate(zone_dict.items()):
        print kk, idx/float(len_dict)
        num_ds = int(kk.split('-jobs')[0].split('_')[1])
        br = float(kk.split('-jobs_')[1].split('-bid')[0])
        sim_df = pd.DataFrame.from_csv(vv['sim'])
        stat_df = pd.DataFrame.from_csv(vv['stat'])
        len_sim = len(sim_df)
        for sim_idx in range(len_sim):
            sim_entry = sim_df.iloc[sim_idx]
            stat_entry = stat_df.iloc[sim_idx]
            bp = br*spot_history.mean()
            sim_start_time = sim_entry['Start time']
            num_interr = sim_entry['Interrupts']
            first_itr_time = sim_entry['First Iter Time']
            total_cost = stat_entry['Total cost']
            instance_cost = stat_entry['Instance cost']
            stor_cost = stat_entry['Storage cost']
            xfer_cost = stat_entry['Tranfer cost']
            total_time = stat_entry['Total time']
            run_time = stat_entry['Run time']
            wait_time = stat_entry['Wait time']
            xfer_up_time = stat_entry['Upload time']
            xfer_down_time = stat_entry['Download time']
            #df_entry = {'sim_idx' : sim_idx, 'av_zone' : av_zone,
            #            'bid_ratio' : br, 'bid_price' : bp, 'num_ds' : num_ds,
            #            'sim_start_time' : sim_start_time,
            #            'num_interrupts' : num_interr,
            #            'first_itr_time' : first_itr_time,
            #            'total_cost' : total_cost, 'instance_cost' : instance_cost,
            #            'stor_cost' : stor_cost, 'xfer_cost' : xfer_cost,
            #            'total_time' : total_time, 'run_time' : run_time,
            #            'wait_time' : wait_time, 'xfer_up_time' : xfer_up_time,
            #            'xfer_down_time' : xfer_down_time}
            #big_df = big_df.append(df_entry, ignore_index=True)
            big_df.loc[loc] = [sim_idx, av_zone, br, bp, num_ds, sim_start_time, num_interr, first_itr_time, total_cost, instance_cost, stor_cost, xfer_cost, total_time, run_time, wait_time, xfer_up_time, xfer_down_time]
            loc += 1
    print 'done!'

    big_df.to_csv('./new/%s.csv' % av_zone)

    print 'done writing!'

def main():
    '''
    '''

    # Import packages
    import glob
    import pandas as pd
    from multiprocessing import Process

    # Init variables
    fp_pattern = './cpac/*/*.csv'
    csvs = glob.glob(fp_pattern)
    var_dict = {}

    # Build variable dictionary
    for csv in csvs:
        csv_sp = csv.split('/')[-1].split('.csv')[0].split('_s')[0]
        zone = csv.split('/')[-2]
        if not var_dict.has_key(zone):
            var_dict[zone] = {}
        if not var_dict[zone].has_key(csv_sp):
            var_dict[zone][csv_sp] = {}
        if 'stat' in csv:
            var_dict[zone][csv_sp]['stat'] = csv
        else:
            var_dict[zone][csv_sp]['sim'] = csv

    # Build big dictionary
    p_list = [Process(target=build_big_df, args=(k, v)) for k, v in var_dict.items()]

    return p_list
