library(ggplot2)
library(plyr)
library(reshape)
library(grid)

# Multiple plot function
#
# ggplot objects can be passed in ..., or to plotlist (as a list of ggplot objects)
# - cols:   Number of columns in layout
# - layout: A matrix specifying the layout. If present, 'cols' is ignored.
#
# If the layout is something like matrix(c(1,2,3,3), nrow=2, byrow=TRUE),
# then plot 1 will go in the upper left, 2 will go in the upper right, and
# 3 will go all the way across the bottom.
#
multiplot <- function(..., plotlist=NULL, file, cols=1, layout=NULL) {
  require(grid)
  
  # Make a list from the ... arguments and plotlist
  plots <- c(list(...), plotlist)
  
  numPlots = length(plots)
  
  # If layout is NULL, then use 'cols' to determine layout
  if (is.null(layout)) {
    # Make the panel
    # ncol: Number of columns of plots
    # nrow: Number of rows needed, calculated from # of cols
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                     ncol = cols, nrow = ceiling(numPlots/cols))
  }
  
  if (numPlots==1) {
    print(plots[[1]])
    
  } else {
    # Set up the page
    grid.newpage()
    pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))
    
    # Make each plot, in the correct location
    for (i in 1:numPlots) {
      # Get the i,j matrix positions of the regions that contain this subplot
      matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))
      
      print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                      layout.pos.col = matchidx$col))
    }
  }
}

calc_costs_aws_2spot <- function(x){
    num_datasets <- x
    num_months = 48

    # constants based on capacity of instances
    # and C-PAC capacity
    datasets_instance <- 3
    hours_dataset <- .75

    # costs of head and node instances
    head_cost_hour <- 0.052
    node_cost_hour <- 2*0.26

    # storage related costs
    EBS_overhead <- 20
    EBS_GB_dataset <- 4.5
    EBS_GB_month <- 0.1
    hours_month <- 30*24
    num_copies <- 3


    #xfers
    xfer_cost_BG <- 0.09


    # calculate the number of instances
    num_instances <- min(c(ceiling(num_datasets/datasets_instance),20))

    # calculate the number of hours
    num_iters <- ceiling(num_datasets / (datasets_instance * num_instances))
    num_hours <- ceiling(hours_dataset * num_iters)

    total_cost <- num_hours * (head_cost_hour + num_instances * node_cost_hour) +
                  num_datasets * EBS_GB_dataset * (num_hours / hours_month) * EBS_GB_month +
                  num_datasets * EBS_GB_dataset * xfer_cost_BG

    total_cost <- if(is.nan(total_cost)) 0 else ceiling(total_cost)
    #return(c(num_instances,num_iters,num_hours,total_cost))
    #return(total_cost,num_hours)
    return(total_cost)
}
calc_costs_aws_spot <- function(x){
  num_datasets <- x
  num_months = 48
  
  # constants based on capacity of instances
  # and C-PAC capacity
  datasets_instance <- 3
  hours_dataset <- .75
  
  # costs of head and node instances
  head_cost_hour <- 0.052
  node_cost_hour <- 0.26
  
  # storage related costs
  EBS_overhead <- 20
  EBS_GB_dataset <- 4.5
  EBS_GB_month <- 0.1
  hours_month <- 30*24
  num_copies <- 3
  
  
  #xfers
  xfer_cost_BG <- 0.09
  
  
  # calculate the number of instances
  num_instances <- min(c(ceiling(num_datasets/datasets_instance),20))
  
  # calculate the number of hours
  num_iters <- ceiling(num_datasets / (datasets_instance * num_instances))
  num_hours <- ceiling(hours_dataset * num_iters)
  
  total_cost <- num_hours * (head_cost_hour + num_instances * node_cost_hour) +
    num_datasets * EBS_GB_dataset * (num_hours / hours_month) * EBS_GB_month +
    num_datasets * EBS_GB_dataset * xfer_cost_BG
  
  total_cost <- if(is.nan(total_cost)) 0 else ceiling(total_cost)
  #return(c(num_instances,num_iters,num_hours,total_cost))
  #return(total_cost,num_hours)
  return(total_cost)
}

calc_costs_aws_ondemand <- function(x){
  num_datasets <- x
  num_months = 48
  
  # constants based on capacity of instances
  # and C-PAC capacity
  datasets_instance <- 3
  hours_dataset <- .75
  
  # costs of head and node instances
  head_cost_hour <- 0.052
  node_cost_hour <- 1.6
  
  # storage related costs
  EBS_overhead <- 20
  EBS_GB_dataset <- 4.5
  EBS_GB_month <- 0.1
  hours_month <- 30*24
  num_copies <- 3
  
  
  #xfers
  xfer_cost_BG <- 0.09
  
  
  # calculate the number of instances
  num_instances <- min(c(ceiling(num_datasets/datasets_instance),20))
  
  # calculate the number of hours
  num_iters <- ceiling(num_datasets / (datasets_instance * num_instances))
  num_hours <- ceiling(hours_dataset * num_iters)
  
  total_cost <- num_hours * (head_cost_hour + num_instances * node_cost_hour) +
    num_datasets * EBS_GB_dataset * (num_hours / hours_month) * EBS_GB_month +
    num_datasets * EBS_GB_dataset * xfer_cost_BG
  
  total_cost <- if(is.nan(total_cost)) 0 else ceiling(total_cost)
  #return(c(num_instances,num_iters,num_hours,total_cost))
  #return(total_cost,num_hours)
  return(total_cost)
}
calc_time_aws <- function(x){
  num_datasets <- x
  num_months = 48
  
  # constants based on capacity of instances
  # and C-PAC capacity
  datasets_instance <- 3
  hours_dataset <- .75
  
  # calculate the number of instances
  num_instances <- min(c(ceiling(num_datasets/datasets_instance),20))
  
  # calculate the number of hours
  num_iters <- ceiling(num_datasets / (datasets_instance * num_instances))
  num_hours <- ceiling(hours_dataset * num_iters)
  
  return(num_hours)
}

calc_time_cap <- function(x){
  num_datasets <- x
  
  # constants based on capacity of instances
  # and C-PAC capacity
  datasets_instance <- 3
  hours_dataset <- .75
  
  # calculate the number of instances
  num_instances <- 1
  
  # calculate the number of hours
  num_iters <- max(ceiling(num_datasets / (datasets_instance * num_instances)),1)
  num_hours <- ceiling(hours_dataset * num_iters)
  
  return(num_hours)
}

calc_costs_cap <- function(x){
  num_datasets <- x
  
  workstation <- 8642
  
  salary <- .05 * 50000 * 1.25
  
  power_supply_kw <- .9*1100/1000
  power_kw <- .1055
    
  # constants based on capacity of instances
  # and C-PAC capacity
  datasets_instance <- 3
  hours_dataset <- .75
  
  # calculate the number of instances
  num_instances <- 1
  
  # calculate the number of hours
  num_iters <- ceiling(num_datasets / (datasets_instance * num_instances))
  num_hours <- ceiling(hours_dataset * num_iters)
  
  total_cost = workstation + salary + num_hours * power_supply_kw * power_kw
  
  return(total_cost)
}


salary <- .05 * 50000 * 1.25
workstation <- 8642
power_cost_h <- .1055 * 1100/1000 *.9

cap_costs <- salary + workstation
costs_compare<-data.frame(num_datasets=seq(0,6000,1))
costs_compare$op_costs_2spot<-apply(costs_compare[1],1,calc_costs_aws_2spot)
costs_compare$op_costs_spot<-apply(costs_compare[1],1,calc_costs_aws_spot)
costs_compare$op_costs_ondemand<-apply(costs_compare[1],1,calc_costs_aws_ondemand)
costs_compare$cap_costs<-apply(costs_compare[1],1,calc_costs_cap)
costs_compare$workstation<-apply(costs_compare[1],1,function(x) workstation)
costs_compare$salary<-apply(costs_compare[1],1,function(x) salary)
costs_compare$electricity<-apply(costs_compare[1],1,function(x) calc_time_cap(x)*power_cost_h)
costs_compare$salary_electricity<-apply(costs_compare[1],1,function(x) calc_time_cap(x)*power_cost_h+salary)
costs_compare_m=melt(costs_compare,id="num_datasets")
costs_compare_m$variable=revalue(costs_compare_m$variable,c("op_costs_2spot"="AWS Spot 2X",
                                                            "op_costs_spot"="AWS Spot",
                                                            "op_costs_ondemand"="AWS On Demand",
                                                            "cap_costs"="Workstation, Maintenance,\nand Electricity",
                                                            "workstation"="Workstation",
                                                            "salary"="Maintenance",
                                                            "electricity"="Electricity",
                                                            "salary_electricity"="Maintenance and\nElectricity"))

time_compare<-data.frame(num_datasets=seq(0,6000,1))
time_compare$cap_time<-apply(costs_compare[1],1,calc_time_cap)
time_compare$op_time<-apply(costs_compare[1],1,calc_time_aws)
time_compare_m=melt(time_compare,id="num_datasets")
time_compare_m$variable=revalue(time_compare_m$variable,c("cap_time"="Local Execution Time",
                                                          "op_time"="AWS Execution Time"))
p1<-ggplot(costs_compare_m)+
  geom_line(aes(num_datasets,value,color=variable))+
  scale_y_continuous(breaks = round(seq(min(costs_compare_m$value), max(costs_compare_m$value), by = 2500),1))+
  xlab("Number of Datasets")+
  ylab("Cost ($)")+
  guides(col = guide_legend(ncol = 2, byrow = FALSE))+
  theme_bw()+
  theme(axis.title.x = element_text(size=6,colour="black"),
        axis.title.y = element_text(size=6,colour="black"),
        axis.text.x = element_text(size=6,colour="black"),
        axis.text.y = element_text(size=6,colour="black"),
        legend.position="bottom",
        legend.title=element_blank(),
        legend.key=element_blank(),
        legend.margin=unit(.1,"cm"),
        legend.key.height=unit(3,"mm"),
        legend.key.width=unit(2,"mm"),
        panel.margin=unit(0,"mm"),
        legend.text = element_text(size=6))

cost_at_1000=costs_compare_m[costs_compare_m$num_datasets==1000,]
#cost_at_1000=apply(cost_at_1000[1],1,function(x) x/1000)

time_at_1000=time_compare_m[time_compare_m$num_datasets==1000,]
#geom_text(aes(x=1000,y=time_at_1000$value,label = round(time_at_1000$value,0),size=11,family="Arial",vjust=-.5))+
p2<-ggplot(time_compare_m)+geom_line(aes(num_datasets,value,color=variable))+
  xlab("Number of Datasets")+
  ylab("Number of Hours")+
  guides(col = guide_legend(ncol = 2, byrow = TRUE))+
  theme_bw()+
  theme(axis.title.x = element_text(size=6,colour="black"),
        axis.title.y = element_text(size=6,colour="black"),
        axis.text.x = element_text(size=6,colour="black"),
        axis.text.y = element_text(size=6,colour="black"),
        legend.position="bottom",
        legend.title=element_blank(),
        legend.key=element_blank(),
        legend.margin=unit(.1,"cm"),
        legend.key.height=unit(3,"mm"),
        legend.key.width=unit(2,"mm"),
        panel.margin=unit(0,"mm"),
        legend.text = element_text(size=6))

p3<-ggplot(cost_at_1000,aes(x=reorder(variable,value,function(x) x),y=value,fill=variable))+
  geom_bar(stat = "identity")+
  geom_text(aes(label=round(value,0),vjust=-.5,size=4))+
  xlab("Approach")+
  ylab("Cost for 1000 datasets ($)")+
  theme_bw()+
  theme(axis.title.x = element_text(size=11,colour="black"),
        axis.title.y = element_text(size=11,colour="black"),
        axis.text.x = element_text(size=11,angle=35,hjust=1,colour="black"),
        axis.text.y = element_text(size=11,colour="black"),
        legend.position="",
        legend.title=element_blank(),
        legend.text = element_text(size=11))
#costs_compare$cap_costs<-apply(costs_compare,1,function (x) cap_costs)

#
pdf(file = "~/Dropbox/_CPAC_BISTI/aws_plot_2mm_1.pdf", width=2.8, height = 3)
print(p1)
dev.off()

pdf(file = "~/Dropbox/_CPAC_BISTI/aws_plot_2mm_2.pdf", width=2.8, height = 3)
print(p2)
dev.off()

pdf(file = "~/Dropbox/_CPAC_BISTI/aws_plot_2mm_3.pdf", width=6, height = 6)
print(p3)
dev.off()
