# spot-model/plot_static_times_costs.R
#
# Author: Cameron Craddock, Daniel Clark (2015)

# Import packages
library(ggplot2)
library(gridExtra)
library(plyr)
library(reshape2)

# Plot and print simulation results to pdf
plot_cost_times <- function(df, out_file) {
  ## Ceiling costs to round to the nearest dollar
  # Cost vs. Bid ratio
  cost_br <- ggplot(df, aes(x=num_datasets, y=ceiling(mean_total_cost), col=av_zone)) +
    labs(x='Number of datasets', y='Total cost ($)',
         title='On-demand costs')
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