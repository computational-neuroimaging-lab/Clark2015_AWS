# spot-model/spot_sim_plots.R
#
# Author: Cameron Craddock, Daniel Clark (2015)

# Import packages
library(ggplot2)
library(gridExtra)
library(plyr)
library(reshape2)

# Create aggregated dataframe
aggregate_df <- function(full_df, func_name) {

  # Check to see if mean or median
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
  df_agg$region[grep("us-west",df_agg$av_zone)]="US West"
  df_agg$region[grep("us-east",df_agg$av_zone)]="US East"
  df_agg$region[grep("ap",df_agg$av_zone)]="Asia Pacific"
  df_agg$region[grep("eu",df_agg$av_zone)]="Europe"
  df_agg$region[grep("sa",df_agg$av_zone)]="S. America"
  #df_agg$region=factor(df_agg$region)

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

### Scatter of simulation versus static models ###
# Init variables
stat_sim_csv <- '~/data/aws/sim_results_merged/03-15_07-10-2015/cpac/mean_static_vs_mean_sim.csv'

# Load in stat vs sim dataframe
stat_sim_df <- read.csv(stat_sim_csv)
stat_sim_df$region <- 'to-fill'
region_stat_sim <- format_region(stat_sim_df)

# Plot
stat_vs_sim <- ggplot(region_stat_sim, aes(x=avg_total_cost, y=total_cost, color=factor(region), size=factor(num_datasets))) +
               labs(x='Avg simulation total cost ($)', y='Static model total cost ($)') + geom_point()

plot(stat_vs_sim)
               
### Mean cost and time vs bid ratio and num datasets ###
# Init variables
df_csv <- '~/data/aws/sim_results_merged/03-15_07-10-2015/cpac/merged_raw_sims-s3_costs.csv'
agg_csv <- 'cpac_df_agg.csv'
avg_type <- 'mean'

# Load in dataframe if it exists
if (!file.exists(agg_csv)) {
  # Read in csv if need be
  if (!exists('full_df')) {
    full_df <- read.csv(df_csv)
  }
  agg_df <- aggregate_df(full_df, avg_type)
} else {
  agg_df <- read.csv(agg_csv)
}

# Cost vs. Bid ratio
cost_br <- ggplot(subset(agg_df, num_datasets==1000),
                  aes(x=bid_ratio, y=ceiling(avg_total_cost), col=av_zone))+
                  geom_line()+
                  facet_grid(region~., scales="free_y")+
                  theme_bw()+
                  theme(legend.position="None",
                        axis.title.x=element_text(size=10,colour="black", vjust=-.8),
                        axis.title.y=element_text(size=10,colour="black"),
                        axis.text.x=element_text(size=8,colour="black", angle=35),
                        axis.text.y=element_text(size=8,colour="black"),
                        strip.text.y=element_text(size = 8, colour = "black"))

# Cost vs. Num datasets
cost_ds <- ggplot(subset(agg_df, bid_ratio=2.5),
                  aes(x=num_datasets, y=ceiling(avg_total_cost), col=av_zone))+
                  geom_line()+
                  facet_grid(region~., scales="free_y")+
                  theme_bw()+
                  theme(legend.position="None",
                        axis.title.x=element_text(size=10,colour="black", vjust=-.8),
                        axis.title.y=element_text(size=10,colour="black"),
                        axis.text.x=element_text(size=8,colour="black", angle=35),
                        axis.text.y=element_text(size=8,colour="black"),
                        strip.text.y=element_text(size = 8, colour = "black"))

# Cost vs. Bid ratio
time_br <- ggplot(subset(agg_df, num_datasets==1000),
                  aes(x=bid_ratio, y=avg_total_time/3600, col=av_zone))+
                  geom_line()+
                  facet_grid(region~., scales="free_y")+
                  theme_bw()+
                  theme(legend.position="None",
                        axis.title.x=element_text(size=10,colour="black", vjust=-.8),
                        axis.title.y=element_text(size=10,colour="black"),
                        axis.text.x=element_text(size=8,colour="black", angle=35),
                        axis.text.y=element_text(size=8,colour="black"),
                        strip.text.y=element_text(size = 8, colour = "black"))

# Cost vs. Num datasets
time_ds <- ggplot(subset(agg_df, bid_ratio=2.5),
                  aes(x=num_datasets, y=avg_total_time/3600, col=av_zone))+
                  geom_line()+
                  facet_grid(region~., scales="free_y")+
                  theme_bw()+
                  theme(legend.position="None",
                        axis.title.x=element_text(size=10,colour="black", vjust=-.8),
                        axis.title.y=element_text(size=10,colour="black"),
                        axis.text.x=element_text(size=8,colour="black", angle=35),
                        axis.text.y=element_text(size=8,colour="black"),
                        strip.text.y=element_text(size = 8, colour = "black"))

pdf(file="cpac_sim.pdf",title="cpac_sim",width=180/25.4,height=8,family="ArialMT",paper="special")

grid.newpage()
layout=grid.layout(2,2)
pushViewport(viewport(layout=layout))

print(cost_br, vp=viewport(layout.pos.row=1, layout.pos.col=1))
print(cost_ds, vp=viewport(layout.pos.row=2, layout.pos.col=1))
print(time_br, vp=viewport(layout.pos.row=1, layout.pos.col=2))
print(time_ds, vp=viewport(layout.pos.row=2, layout.pos.col=2))

dev.off()

