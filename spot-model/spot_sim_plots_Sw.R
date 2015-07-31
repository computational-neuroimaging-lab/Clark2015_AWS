# spot-model/spot_sim_plots.R
#
# Author: Cameron Craddock, Daniel Clark (2015)

# Import packages
library(ggplot2)
library(gridExtra)
library(plyr)
library(reshape2)

# Init variables
df_csv <- '~/data/aws/sim_results_merged/03-15_07-10-2015/cpac/merged_raw_sims-s3_costs1.csv'
agg_csv <- 'cpac_df_agg.csv'

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
      df_agg <- ddply(full_df, .(av_zone, bid_ratio), summarize,
                      avg_instance_cost=median(instance_cost),
                      avg_num_interr=median(num_interrupts),
                      avg_pernode_cost=median(per_node_cost),
                      avg_run_time=median(run_time),
                      avg_total_cost=median(total_cost),
                      avg_total_time=median(total_time),
                      avg_wait_time=median(wait_time))
    }
    # Mean statistics by av_zone, num_datasets, and bid_ratio
#     df_agg <- ddply(full_df,.(Av.zone,Num.datasets,Bid.ratio),
#                          summarize,
#                          mean_total_cost=median(Total.cost),
#                          mean_instance_cost=median(Instance.cost),
#                          mean_storage_cost=median(Storage.cost),
#                          mean_xfer_cost=median(Tranfer.cost),
#                          mean_total_time=median(Total.time),
#                          mean_run_time=median(Run.time),
#                          mean_wait_time=median(Wait.time),
#                          mean_upl_time=median(Upload.time),
#                          mean_dl_time=median(Download.time),
#                          mean_interrupts=median(Interrupts))

    
    
    # Remove the full df
    # rm(full_df)

    # Rename regions
    df_agg$Region[grep("us-west",df_agg$av_zone)]="US West"
    df_agg$Region[grep("us-east",df_agg$av_zone)]="US East"
    df_agg$Region[grep("ap",df_agg$av_zone)]="Asia Pacific"
    df_agg$Region[grep("eu",df_agg$av_zone)]="Europe"
    df_agg$Region[grep("sa",df_agg$av_zone)]="S. America"
    df_agg$Region=factor(df_agg$Region)
    
    # Return the aggregated dataframe
    return(df_agg)
}

# Read in csv if need be
if (!exists('full_df')) {
    full_df <- read.csv(df_csv)
}

agg_df <- aggregate_df(full_df, 'mean')

# Load in dataframe if it exists
if (!file.exists(agg_csv)) {
    agg_df <- aggregate_df(df_csv)
    #cpac_df_agg=subset(cpac_df_agg,Num.datasets<=10000)
    #cpac_df_agg=subset(cpac_df_agg,Num.datasets < 3000)
    write.csv(agg_df, file="cpac_df_agg.csv")
} else {
    agg_df <- read.csv("cpac_df_agg.csv")
    #cpac_df_agg=subset(cpac_df_agg,Num.datasets < 3000)
}

# Create plots
p1<-ggplot(subset(agg_df,Num.datasets==1000), 
           aes(x=Bid.ratio,y=ceiling(mean_total_cost),col=Av.zone))+
  geom_line()+
  facet_grid(Region~.,scales="free_y")+
  theme_bw()+
  theme(legend.position="None",
        axis.title.x = element_text(size=10,colour="black", vjust=-.8),
        axis.title.y = element_text(size=10,colour="black"),
        axis.text.x = element_text(size=8,colour="black", angle=35),
        axis.text.y = element_text(size=8,colour="black"),
        strip.text.y = element_text(size = 8, colour = "black"))


p2<-ggplot(subset(agg_df,Num.datasets==1000), 
           aes(x=Bid.ratio,y=mean_total_time/3600,col=Av.zone))+
  geom_line()+
  facet_grid(Region~.,scales="free_y")+
  theme(legend.position="None")

p3<-ggplot(subset(agg_df,Bid.ratio==2.5), 
           aes(x=Num.datasets,y=mean_total_cost,col=Av.zone))+
  geom_line()+
  facet_grid(Region~.,scales="free_y")+
  theme(legend.position="None")

p4<-ggplot(subset(agg_df,Bid.ratio==2.5), 
           aes(x=Num.datasets,y=mean_total_time/3600,col=Av.zone))+
  geom_line()+
  facet_grid(Region~.,scales="free_y")+
  theme(legend.position="None")

p5<-ggplot(subset(agg_df,Bid.ratio==2.5), 
           aes(x=Num.datasets,y=mean_interrupts,col=Av.zone))+
  geom_line()+
  facet_grid(Region~.,scales="free_y")+
  theme(legend.position="None")


pdf(file="cpac_sim.pdf",title="cpac_sim",width=180/25.4,height=8,family="ArialMT",paper="special")

grid.newpage()
layout=grid.layout(2,2)
pushViewport(viewport(layout=layout))

print(p1,vp=viewport(layout.pos.row=1, layout.pos.col=1))
print(p2,vp=viewport(layout.pos.row=2, layout.pos.col=1))
print(p3,vp=viewport(layout.pos.row=1, layout.pos.col=2))
print(p4,vp=viewport(layout.pos.row=2, layout.pos.col=2))

dev.off()

