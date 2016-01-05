# spot-model/plot_static_times_costs.R
#
# Author: Cameron Craddock, Daniel Clark (2015)

# Import packages
library(ggplot2)
library(gridExtra)
library(plyr)
library(reshape2)


# Populate regions with name formatted
format_region <- function(data_frame) {
  data_frame$region[grep('us-west-1',data_frame$av_zone)]='US West (N. California)'
  data_frame$region[grep('us-west-2',data_frame$av_zone)]='US West (Oregon)'
  data_frame$region[grep('us-east-1',data_frame$av_zone)]='US East (N. Virginia)'
  data_frame$region[grep('eu-west-1',data_frame$av_zone)]='Europe (Ireland)'
  data_frame$region[grep('eu-central-1',data_frame$av_zone)]='Europe (Frankfurt)'
  data_frame$region[grep('ap-southeast-1',data_frame$av_zone)]='Asia Pacific (Singapore)'
  data_frame$region[grep('ap-southeast-2',data_frame$av_zone)]='Asia Pacific (Sydney)'
  data_frame$region[grep('ap-northeast-1',data_frame$av_zone)]='Asia Pacific (Tokyo)'
  data_frame$region[grep('sa-east-1',data_frame$av_zone)]='S. America (Sao Paulo)'
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

# Format cost/time vs num_ds/bid_ratio plots
breakout_bid_ratios <- function(plot_obj) {
  
  # Add ggplot2 formatting to plot object
  frmt_plot <- plot_obj +
    facet_grid(bid_ratio~., scales='free_y') +
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
plot_ondemand <- function(df, out_file) {

  # Get on demand costs plot
  ondemand_cost <- ggplot(df, aes(x=num_datasets, y=on_demand_total_cost, col=region)) +
    labs(x='Number of datasets', y='Total cost ($)',
         title='On-demand costs') + geom_line()
  #ondemand_cost <- format_cost_times(ondemand_cost)
  
  # Get on demand costs plot
  ondemand_time <- ggplot(df, aes(x=num_datasets, y=static_total_time/3600.0, col='all regions')) +
    labs(x='Number of datasets', y='Total time (hrs)',
         title='On-demand times') + geom_line()
  #ondemand_time <- format_cost_times(ondemand_time)
  
  # Open pdf file to save plots to
  pdf(file=out_file, title='ondemand', width=8, height=180/25.4,
      family='ArialMT', paper='special')
  
  # Set up the 2x2 grid
  grid.newpage()
  layout=grid.layout(2,1)
  pushViewport(viewport(layout=layout))
  
  # Print to pdf
  print(ondemand_cost, vp=viewport(layout.pos.row=1, layout.pos.col=1))
  print(ondemand_time, vp=viewport(layout.pos.row=2, layout.pos.col=1))
  
  # Shutdown printout device
  dev.off()
}




# Plot the costs vs times
plot_cost_vs_times <- function(df, out_file) {
  on_demand <- ggplot(df, aes(x=static_total_time/3600.0,
                              y=on_demand_total_cost, col=region)) +
    labs(x='Total time (hrs)', y='Total cost ($)', title='On-demand cost vs time') +
    geom_point(alpha=0.2)
  
  spot <- ggplot(df, aes(x=mean_total_time/3600.0, y=mean_total_cost, col=region)) +
    labs(x='Total time (hrs)', y='Total cost ($)', title='Mean spot cost vs time') +
    geom_point(alpha=0.2)
  
  pdf(file=out_file, title='time vs cost', width=8, height=180/25.4,
      family='ArialMT', paper='special')
  
  grid.newpage()
  layout=grid.layout(2,1)
  pushViewport(viewport(layout=layout))
  
  print(on_demand, vp=viewport(layout.pos.row=1, layout.pos.col=1))
  print(spot, vp=viewport(layout.pos.row=2, layout.pos.col=1))
  
  dev.off()
}

plot_cost_vs_times_br <- function(df, out_file) {
  spot <- ggplot(df, aes(x=mean_total_time/3600.0, y=mean_total_cost, col=region)) +
    labs(x='Total time (hrs)', y='Total cost ($)', title='Mean spot cost vs time') +
    geom_point(alpha=0.2)
  
  pdf(file=out_file, title='time vs cost br', width=8, height=180/25.4,
      family='ArialMT', paper='special')
  spot <- breakout_bid_ratios(spot)
#   grid.newpage()
#   layout=grid.layout(1,1)
#   pushViewport(viewport(layout=layout))
  
  print(spot)#, vp=viewport(layout.pos.row=1, layout.pos.col=1))
  dev.off()
}


plot_br_violin <- function(df, out_file) {
  br_time <- ggplot(df, aes(x=factor(bid_ratio), y=total_time/3600.0, col=bid_ratio)) +
    labs(x='Bid ratio', y='Total time (hrs)', title='Time distribution vs bid ratio') +
    geom_violin()
  br_cost <- ggplot(df, aes(x=factor(bid_ratio), y=total_cost, col=bid_ratio)) +
    labs(x='Bid ratio', y='Total cost ($)', title='Cost distribution vs bid ratio') +
    geom_violin(alpha=0.2)
  
  pdf(file=out_file, title='br', width=8, height=180/25.4,
      family='ArialMT', paper='special')
  
  grid.newpage()
  layout=grid.layout(2,1)
  pushViewport(viewport(layout=layout))
  
  print(br_time, vp=viewport(layout.pos.row=1, layout.pos.col=1))
  print(br_cost, vp=viewport(layout.pos.row=2, layout.pos.col=1))
  
  dev.off()
}

plot_av_violin <- function(df, out_file) {

  av_time <- ggplot(df, aes(x=factor(av_zone), y=total_time/3600.0, col=region)) +
    labs(x='Avail. zone', y='Total time (hrs)', title='Time distribution vs avail. zone') +
    geom_violin(alpha=0.2) + theme(axis.text.x=element_blank())
  av_cost <- ggplot(df, aes(x=factor(av_zone), y=total_cost, col=region)) +
    labs(x='Avail. zone', y='Total cost ($)', title='Cost distribution vs avail. zone') +
    geom_violin(alpha=0.2) + theme(axis.text.x=element_blank())

  pdf(file=out_file, title='av', width=8, height=180/25.4,
      family='ArialMT', paper='special')
  
  grid.newpage()
  layout=grid.layout(2,1)
  pushViewport(viewport(layout=layout))

  print(av_time, vp=viewport(layout.pos.row=1, layout.pos.col=1))
  print(av_cost, vp=viewport(layout.pos.row=2, layout.pos.col=1))
  
  dev.off()
}

# Init variables
# Local dir of project base on computer
proj_base_dir <- '~/Documents/projects/Clark2015_AWS'
# Relative path of project csvs
rel_csvs_dir <- 'spot-model/csvs'

# Input parameters
# Pipeline
pipeline <- 'cpac'
# # Plotting parameters
# bid_ratio = 2.5
# num_datasets = 1000

# Define csv
merged_csv <- file.path(proj_base_dir, rel_csvs_dir,
                          paste(pipeline, '_merged.csv', sep=''))

# Out pdf
out_pdf <- file.path(proj_base_dir, 'spot-model/plots', paste(pipeline, '_ondemand.pdf', sep=''))

# Load in sim vs stat dataframe
merged_df <- read.csv(merged_csv)
merged_df$region = ''
region_df <- format_region(merged_df)

# Plot
plot_ondemand(region_df, out_pdf)