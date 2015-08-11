# spot-model/spot_sim_plots.R
#
# Author: Cameron Craddock, Daniel Clark (2015)

# Import packages
library(ggplot2)
library(gridExtra)
library(plyr)
library(reshape2)

# Create aggregated dataframe
aggregate_df <- function(csv, func_name) {
  
  # Read in dataframe
  print('Reading in full dataframe...')
  full_df <- read.csv(csv)
  
  # Check to see if mean or median
  print('Aggregating dataframe...')
  if (func_name == 'mean') {
    df_agg <- ddply(full_df, .(av_zone, bid_ratio, num_datasets), summarize,
                    avg_instance_cost=mean(instance_cost),
                    avg_num_interr=mean(num_interrupts),
                    avg_pernode_cost=mean(per_node_cost),
                    avg_run_time=mean(run_time),
                    avg_total_cost=mean(total_cost),
                    avg_total_time=mean(total_time),
                    avg_wait_time=mean(wait_time))
  } else if (func_name == 'median') {
      df_agg <- ddply(full_df, .(av_zone, bid_ratio, num_datasets), summarize,
                      avg_instance_cost=median(instance_cost),
                      avg_num_interr=median(num_interrupts),
                      avg_pernode_cost=median(per_node_cost),
                      avg_run_time=median(run_time),
                      avg_total_cost=median(total_cost),
                      avg_total_time=median(total_time),
                      avg_wait_time=median(wait_time))
  }

  # Rename regions
  df_agg <- format_region(df_agg)

  # Return the aggregated dataframe
  return(df_agg)
}


# Populate regions with name formatted
format_region <- function(data_frame) {
  data_frame$region[grep("us-west",data_frame$av_zone)]="US West"
  data_frame$region[grep("us-east",data_frame$av_zone)]="US East"
  data_frame$region[grep("ap",data_frame$av_zone)]="Asia Pacific"
  data_frame$region[grep("eu",data_frame$av_zone)]="Europe"
  data_frame$region[grep("sa",data_frame$av_zone)]="S. America"
  #data_frame$region=factor(data_frame$region)
  
  return(data_frame)
}


# Format cost/time vs num_ds/bid_ratio plots
format_cost_times <- function(plot_obj) {

  # Add ggplot2 formatting to plot object
  frmt_plot <- plot_obj +
    geom_line() +
    facet_grid(region~., scales='free_y') +
    theme_bw() +
    theme(legend.position='None',
          axis.title.x=element_text(size=10, colour='black', vjust=-.8),
          axis.title.y=element_text(size=10, colour='black'),
          axis.text.x=element_text(size=8, colour='black', angle=35),
          axis.text.y=element_text(size=8, colour='black'),
          strip.text.y=element_text(size=8, colour='black'))
  
  # Return formatted plot
  return(frmt_plot)
}

# Plot and print simulation results to pdf
plot_cost_times <- function(agg_df, num_ds, bid_rat, out_file) {
  # Cost vs. Bid ratio
  cost_br <- ggplot(subset(agg_df, num_datasets==1000),
                    aes(x=bid_ratio, y=ceiling(avg_total_cost), col=av_zone))
  cost_br <- format_cost_times(cost_br)
  
  # Cost vs. Num datasets
  cost_ds <- ggplot(subset(agg_df, bid_ratio==2.5),
                    aes(x=num_datasets, y=ceiling(avg_total_cost), col=av_zone))
  cost_ds <- format_cost_times(cost_ds)
  
  # Cost vs. Bid ratio
  time_br <- ggplot(subset(agg_df, num_datasets==1000),
                    aes(x=bid_ratio, y=avg_total_time/3600, col=av_zone))
  time_br <- format_cost_times(time_br)
  
  # Cost vs. Num datasets
  time_ds <- ggplot(subset(agg_df, bid_ratio==2.5),
                    aes(x=num_datasets, y=avg_total_time/3600, col=av_zone))
  time_ds <- format_cost_times(time_ds)
  
  # Open pdf file to save plots to
  pdf(file=out_file, title='sim_results', width=180/25.4, height=8,
      family='ArialMT', paper='special')
  
  # Set up the 2x2 grid
  grid.newpage()
  layout=grid.layout(2,2)
  pushViewport(viewport(layout=layout))
  
  # Print to pdf
  print(cost_br, vp=viewport(layout.pos.row=1, layout.pos.col=1))
  print(cost_ds, vp=viewport(layout.pos.row=2, layout.pos.col=1))
  print(time_br, vp=viewport(layout.pos.row=1, layout.pos.col=2))
  print(time_ds, vp=viewport(layout.pos.row=2, layout.pos.col=2))
  
  # Shutdown printout device
  dev.off()
}

### Scatter of simulation versus static models ###
# Init variables
stat_sim_csv <- '~/data/aws/sim_results_merged/03-15_07-10-2015/cpac/median_static_vs_median_sim.csv'

# Load in stat vs sim dataframe
stat_sim_df <- read.csv(stat_sim_csv)
stat_sim_df$region <- 'to-fill'
region_stat_sim <- format_region(stat_sim_df)

# Plot
stat_vs_sim_cost <- ggplot(region_stat_sim, aes(x=total_cost, y=avg_total_cost, color=factor(region), size=factor(num_datasets))) +
  labs(x='Static model total cost ($)', y='Avg simulation total cost ($)') + geom_point()

stat_vs_sim_time <- ggplot(region_stat_sim, aes(x=total_time/3600, y=avg_total_time/3600, color=factor(region), size=factor(num_datasets))) +
  labs(x='Static model total time (hrs)', y='Avg simulation total time (hrs)') + geom_point()

plot(stat_vs_sim_cost)
plot(stat_vs_sim_time)

### Mean cost and time vs bid ratio and num datasets ###
# Init variables
full_csv <- '~/data/aws/sim_results_merged/03-15_07-10-2015/cpac/merged_raw_sims-s3_costs.csv'
agg_csv <- '~/data/aws/sim_results_merged/03-15_07-10-2015/cpac/merged_mean-s3_costs.csv'
avg_type <- 'median'

# Form the averaged-aggregated-dataframe
#agg_df <- aggregate_df(full_csv, avg_type)
agg_df <- read.csv(agg_csv)
agg_df <- format_region(agg_df)
plot_cost_times(agg_df, 0, 0, 'cpac_sim_mean.pdf')
