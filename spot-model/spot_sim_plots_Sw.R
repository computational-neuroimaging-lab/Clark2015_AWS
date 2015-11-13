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
                    mean_instance_cost=mean(instance_cost),
                    mean_num_interr=mean(num_interrupts),
                    mean_pernode_cost=mean(per_node_cost),
                    mean_run_time=mean(run_time),
                    mean_total_cost=mean(total_cost),
                    mean_total_time=mean(total_time),
                    mean_wait_time=mean(wait_time))
  } else if (func_name == 'median') {
      df_agg <- ddply(full_df, .(av_zone, bid_ratio, num_datasets), summarize,
                      median_instance_cost=median(instance_cost),
                      median_num_interr=median(num_interrupts),
                      median_pernode_cost=median(per_node_cost),
                      median_run_time=median(run_time),
                      median_total_cost=median(total_cost),
                      median_total_time=median(total_time),
                      median_wait_time=median(wait_time))
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

  # Return the data frame with region header
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
  ## Ceiling costs to round to the nearest dollar
  # Cost vs. Bid ratio
  cost_br <- ggplot(subset(agg_df, num_datasets==num_ds),
                    aes(x=bid_ratio, y=ceiling(mean_total_cost), col=av_zone)) +
             labs(x='Bid ratio to mean price', y='Cost ($)',
                  title=paste('Cost vs bid ratio, datasets = ', num_ds))
  cost_br <- format_cost_times(cost_br)

  # Cost vs. Num datasets
  cost_ds <- ggplot(subset(agg_df, bid_ratio==bid_rat),
                    aes(x=num_datasets, y=ceiling(mean_total_cost), col=av_zone)) +
             labs(x='Number of datasets', y='Cost ($)',
                  title=paste('Cost vs datasets, bid ratio = ', bid_rat))
  cost_ds <- format_cost_times(cost_ds)

  # Cost vs. Bid ratio
  time_br <- ggplot(subset(agg_df, num_datasets==num_ds),
                    aes(x=bid_ratio, y=mean_total_time/3600, col=av_zone)) +
             labs(x='Bid ratio to mean spot price', y='Time (hours)',
                  title=paste('Time vs bid ratio, datasets = ', num_ds))
  time_br <- format_cost_times(time_br)

  # Cost vs. Num datasets
  time_ds <- ggplot(subset(agg_df, bid_ratio==bid_rat),
                    aes(x=num_datasets, y=mean_total_time/3600, col=av_zone)) +
             labs(x='Number of datasets', y='Time (hours)',
                  title=paste('Time vs datasets, bid ratio = ', bid_rat))
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
# Local dir of project base on computer
proj_base_dir <- '~/Documents/projects/Clark2015_AWS'
# Relative path of project csvs
rel_csvs_dir <- 'spot-model/csvs'


ants_csv <- file.path(proj_base_dir, rel_csvs_dir, 'ants_avg_sims_and_static.csv')
cpac_csv <- file.path(proj_base_dir, rel_csvs_dir, 'cpac_avg_sims_and_static.csv')
fs_csv <- file.path(proj_base_dir, rel_csvs_dir, 'fs_avg_sims_and_static.csv')

# Load in stat vs sim dataframe
ants_df <- read.csv(ants_csv)
cpac_df <- read.csv(cpac_csv)
fs_df <- read.csv(fs_csv)

# Plotting parameters
bid_ratio = 2.5
num_datasets = 1000

# To write out
plot_cost_times(ants_df, num_datasets, bid_ratio,
                file.path(proj_base_dir, 'spot-model/plots', 'ants_sim_mean.pdf'))
plot_cost_times(cpac_df, num_datasets, bid_ratio,
                file.path(proj_base_dir, 'spot-model/plots', 'cpac_sim_mean.pdf'))
plot_cost_times(fs_df, num_datasets, bid_ratio,
                file.path(proj_base_dir, 'spot-model/plots', 'fs_sim_mean.pdf'))

# Plot
# Average sim vs static costs
stat_vs_sim_cost <- ggplot(subset(ants_df, bid_ratio=bid_ratio), 
                           aes(x=static_total_cost, y=mean_total_cost,
                               color=factor(region), size=factor(num_datasets))) +
                    labs(x='Static model total cost ($)',
                         y='Mean simulation total cost ($)',
                         title=paste('Mean simulation costs vs Static model costs, bid ratio =', bid_ratio)) +
                    geom_point(alpha=2/10)

pdf(file=file.path(proj_base_dir, 'spot-model/plots', 'ants_mean_sim_vs_static_costs.pdf'),
    width=11, height=8)
print(stat_vs_sim_cost)
dev.off()

# Average sim vs static times
stat_vs_sim_time <- ggplot(subset(ants_df, bid_ratio=bid_ratio),
                           aes(x=static_total_time/3600,y=mean_total_time/3600,
                               color=factor(region), size=factor(num_datasets))) +
                           labs(x='Static model total time (hrs)',
                                y='Mean simulation total time (hrs)',
                                title=paste('Mean simulation time vs Static model time, bid ratio =', bid_ratio)) +
                           geom_point(alpha=2/10)


pdf(file=file.path(proj_base_dir, 'spot-model/plots', 'ants_mean_sim_vs_static_times.pdf'),
    width=11, height=8)
print(stat_vs_sim_time)
dev.off()

