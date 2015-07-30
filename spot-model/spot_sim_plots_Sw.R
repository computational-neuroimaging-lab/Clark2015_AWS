library(ggplot2)
library(reshape)
library(plyr)

if( ! file.exists("cpac_df_agg.csv")){
  cpac_df = read.csv("cpac_df.csv")
  
  summary(cpac_df)
  
  # Mean statistics by av_zone, num_datasets, and bid_ratio
  cpac_df_agg<-ddply(cpac_df,.(Av.zone,Num.datasets,Bid.ratio),
                     summarize,
                     mean_total_cost=median(Total.cost),
                     mean_instance_cost=median(Instance.cost),
                     mean_storage_cost=median(Storage.cost),
                     mean_xfer_cost=median(Tranfer.cost),
                     mean_total_time=median(Total.time),
                     mean_run_time=median(Run.time),
                     mean_wait_time=median(Wait.time),
                     mean_upl_time=median(Upload.time),
                     mean_dl_time=median(Download.time),
                     mean_interrupts=median(Interrupts))
  
  
  # remove the full df
  rm(cpac_df)
  
  cpac_df_agg$Region[grep("us-west",cpac_df_agg$Av.zone)]="US West"
  cpac_df_agg$Region[grep("us-east",cpac_df_agg$Av.zone)]="US East"
  cpac_df_agg$Region[grep("ap",cpac_df_agg$Av.zone)]="Asia Pacific"
  cpac_df_agg$Region[grep("eu",cpac_df_agg$Av.zone)]="Europe"
  cpac_df_agg$Region[grep("sa",cpac_df_agg$Av.zone)]="S. America"
  cpac_df_agg$Region=factor(cpac_df_agg$Region)
  #cpac_df_agg=subset(cpac_df_agg,Num.datasets<=10000)
  cpac_df_agg=subset(cpac_df_agg,Num.datasets < 3000)
  write.csv(cpac_df_agg,file="cpac_df_agg.csv")
} else {
  cpac_df_agg=read.csv("cpac_df_agg.csv")
  cpac_df_agg=subset(cpac_df_agg,Num.datasets < 3000)
}

library(gridExtra)

p1<-ggplot(subset(cpac_df_agg,Num.datasets==1000), 
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


p2<-ggplot(subset(cpac_df_agg,Num.datasets==1000), 
           aes(x=Bid.ratio,y=mean_total_time/3600,col=Av.zone))+
  geom_line()+
  facet_grid(Region~.,scales="free_y")+
  theme(legend.position="None")

p3<-ggplot(subset(cpac_df_agg,Bid.ratio==2.5), 
           aes(x=Num.datasets,y=mean_total_cost,col=Av.zone))+
  geom_line()+
  facet_grid(Region~.,scales="free_y")+
  theme(legend.position="None")

p4<-ggplot(subset(cpac_df_agg,Bid.ratio==2.5), 
           aes(x=Num.datasets,y=mean_total_time/3600,col=Av.zone))+
  geom_line()+
  facet_grid(Region~.,scales="free_y")+
  theme(legend.position="None")

p5<-ggplot(subset(cpac_df_agg,Bid.ratio==2.5), 
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

