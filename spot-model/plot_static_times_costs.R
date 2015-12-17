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
plot_ondemand_static <- function(df, out_file) {

  # Get on demand costs plot
  ondemand_cost <- ggplot(df, aes(x=num_datasets, y=on_demand_total_cost, col=av_zone)) +
    labs(x='Number of datasets', y='Total cost ($)',
         title='On-demand costs')
  ondemand_cost <- format_cost_times(ondemand_cost)
  
  # Get on demand costs plot
  static_cost <- ggplot(subset(df, bid_ratio==1.0), aes(x=num_datasets, y=static_total_cost, col=av_zone)) +
    labs(x='Number of datasets', y='Total cost ($)',
         title='Static costs (1.0x bid ratio)')
  static_cost <- format_cost_times(static_cost)
  
  # Open pdf file to save plots to
  pdf(file=out_file, title='ondemand_static', width=180/25.4, height=8,
      family='ArialMT', paper='special')
  
  # Set up the 2x2 grid
  grid.newpage()
  layout=grid.layout(2,1)
  pushViewport(viewport(layout=layout))
  
  # Print to pdf
  print(ondemand_cost, vp=viewport(layout.pos.row=1, layout.pos.col=1))
  print(static_cost, vp=viewport(layout.pos.row=2, layout.pos.col=1))
  
  # Shutdown printout device
  dev.off()
}

# Init variables
# Local dir of project base on computer
proj_base_dir <- '~/Documents/projects/Clark2015_AWS'
# Relative path of project csvs
rel_csvs_dir <- 'spot-model/csvs'

# Input parameters
# Pipeline
pipeline <- 'fs'
# # Plotting parameters
# bid_ratio = 2.5
# num_datasets = 1000

# Define csv
merged_csv <- file.path(proj_base_dir, rel_csvs_dir,
                          paste(pipeline, '_merged.csv', sep=''))

# Out pdf
out_pdf <- file.path(proj_base_dir, 'spot-model/plots', paste(pipeline, '_ondemand_static.pdf', sep=''))

# Load in sim vs stat dataframe
merged_df <- read.csv(merged_csv)

region_df <- format_region(merged_df)

# Plot
plot_ondemand_static(region_df, out_pdf)