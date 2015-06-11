# spot_sim_plots.R
#
# Author: Daniel Clark, 2015

# Import packages
library(ggplot2)
library(plyr)

# Init variables
df_csv <- '/home/dclark/Documents/projects/Clark2015_AWS/spot-model/cpac_df.csv'
bid_ratio <- 2.5
num_datasets <- 1000

# Read in dataframe
df <- read.csv(df_csv)

# Get fixed bid ratio data frame for plotting
fixed_bid_df <- subset(df, Bid.ratio == bid_ratio)

# Get fixed dataset size frame for plotting
fixed_ds_df <- subset(df, Num.datasets == num_datasets)

# Get mean total cost and total time dataframes for fixed bid ratio
mean_total_cost_df <- ddply(fixed_bid_df, .(Av.zone, Num.datasets), summarize, mean_cost=mean(Total.cost))
mean_total_time_df <- ddply(fixed_bid_df, .(Av.zone, Num.datasets), summarize, mean_cost=mean(Total.time))

# Plot total cost vs. num of datasets
ggplot(mean_total_cost_df, aes(x=Num.datasets, y=mean_cost, colour=Av.zone)) + geom_line()
ggplot(mean_total_time_df, aes(x=Num.datasets, y=mean_cost, colour=Av.zone)) + geom_line()

# Get mean total cost and total time dataframes for fixed num datasets
mean_total_cost_df <- ddply(fixed_ds_df, .(Av.zone, Bid.ratio), summarize, mean_cost=mean(Total.cost))
mean_total_time_df <- ddply(fixed_ds_df, .(Av.zone, Bid.ratio), summarize, mean_cost=mean(Total.time))

# Plot total cost vs. num of datasets
ggplot(mean_total_cost_df, aes(x=Bid.ratio, y=mean_cost, colour=Av.zone)) + geom_line()
ggplot(mean_total_time_df, aes(x=Bid.ratio, y=mean_cost, colour=Av.zone)) + geom_line()