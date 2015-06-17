# spot_sim_plots.R
#
# Author: Daniel Clark, 2015

# Import packages
library(ggplot2)
library(plyr)
library(reshape2)
library(grid)

# Init variables
cpac_csv <- '/home/dclark/Documents/projects/Clark2015_AWS/spot-model/sim-outs/cpac_df.csv'
ants_csv <- '/home/dclark/Documents/projects/Clark2015_AWS/spot-model/sim-outs/ants_df.csv'
fs_csv <- '/home/dclark/Documents/projects/Clark2015_AWS/spot-model/sim-outs/fs_df.csv'

# Sim plots fixed variables
bid_ratio <- 2.5
num_datasets <- 1000

# Simulation plots
sim_plots <- function(csv, bid_ratio, num_datasets){
    # Read in dataframe
    df <- read.csv(csv)

    # Get fixed bid ratio data frame for plotting
    fixed_bid_df <- subset(df, Bid.ratio == bid_ratio)

    # Get fixed dataset size frame for plotting
    fixed_ds_df <- subset(df, Num.datasets == num_datasets)

    # Get mean total cost and total time dataframes for fixed bid ratio
    mean_total_cost_df <- ddply(fixed_bid_df, .(Av.zone, Num.datasets), summarize, mean_cost=mean(Total.cost))
    mean_total_time_df <- ddply(fixed_bid_df, .(Av.zone, Num.datasets), summarize, mean_time=mean(Total.time))

    # Plot total cost vs. num of datasets
    p0 <- ggplot(mean_total_cost_df, aes(x=Num.datasets, y=mean_cost, colour=Av.zone)) + geom_line()
    p1 <- ggplot(mean_total_time_df, aes(x=Num.datasets, y=mean_time, colour=Av.zone)) + geom_line()

    # Get mean total cost and total time dataframes for fixed num datasets
    mean_total_cost_df <- ddply(fixed_ds_df, .(Av.zone, Bid.ratio), summarize, mean_cost=mean(Total.cost))
    mean_total_time_df <- ddply(fixed_ds_df, .(Av.zone, Bid.ratio), summarize, mean_time=mean(Total.time))

    # Plot total cost vs. num of datasets
    p2 <- ggplot(mean_total_cost_df, aes(x=Bid.ratio, y=mean_cost, colour=Av.zone)) + geom_line()
    p3 <- ggplot(mean_total_time_df, aes(x=Bid.ratio, y=mean_time, colour=Av.zone)) + geom_line()
    
    p_list <- list(p0, p1, p2, p3)
    
    # Return list of plots
    return(p_list)
}

# p_list <- sim_plots(cpac_csv, bid_ratio, num_datasets)
# 
# print(p_list[1])
# print(p_list[2])
# print(p_list[3])
# print(p_list[4])

# Save pdfs
# pdf(file = "~/Documents/projects/Clark2015_AWS/poster/mean-costs_1000ds-cpac.pdf", width=5, height = 5)
# print(p_list[3])
# dev.off()
# pdf(file = "~/Documents/projects/Clark2015_AWS/poster/mean-times_1000ds-cpac.pdf", width=5, height = 5)
# print(p_list[4])
# dev.off()


# Calculate captial costs
calc_costs_cap <- function(num_datasets, jobs_per, num_mins){

    # Init variables
    workstation <- 8642
    salary <- .05 * 50000 * 1.25
    power_supply_kw <- .9*1100/1000
    power_kw <- .1055

    # Compute hours from minutes
    num_hours = num_mins/60.0

    # Calculate the number of hours
    num_iters <- ceiling(num_datasets / jobs_per)
    num_hours <- ceiling(num_hours * num_iters)
    total_cost = workstation + salary + num_hours * power_supply_kw * power_kw

    # Return total cost
    return(total_cost)
}


# Calculate capital time
calc_time_cap <- function(num_datasets, jobs_per, num_mins){

    # Comput hours from minutes
    num_hours <- num_mins/60.0

    # Calculate the number of hours
    num_iters <- max(ceiling(num_datasets/jobs_per), 1)
    num_hours <- ceiling(num_hours * num_iters)

    # Return total time
    return(num_hours)
}


# Costs dataframe and plots
costs_df_plots <- function(df, max_num, costs_compare){

    # Reshape data
    costs_df <- melt(df, id=c('X', 'Download.time', 'Upload.time', 'Wait.time',
                              'Run.time', 'Total.cost', 'Total.time', 'Sim.index',
                              'Av.zone', 'Bid.ratio', 'Bid.price', 'Num.datasets',
                              'Start.time', 'Interrupts', 'First.Iter.Time'))

    # Get the levels for variable in required order
    costs_df$variable <- factor(costs_df$variable, levels=c('Instance.cost', 'Storage.cost', 'Tranfer.cost'))
    costs_df <- arrange(costs_df, Num.datasets, variable)

    # Calculate the percentages
    costs_df <- ddply(costs_df, .(Num.datasets), transform, percent=value/sum(value)*100)

    # Format the labels and calculate their positions
    costs_df <- ddply(costs_df, .(Num.datasets), transform, pos=(cumsum(value) - 0.5*value))
    costs_df$label <- paste0(sprintf("%.0f", costs_df$percent), "%")

    # Get all data under maxnum datasets
    costs_maxnum <- subset(costs_df, Num.datasets <= max_num)
    cap_costs_maxnum <- subset(costs_compare, num_datasets <= max_num)

    # Initial plot
    p0 <- ggplot() +
          geom_bar(data=costs_maxnum,
                   aes(x=factor(Num.datasets), y=value, fill=variable),
                   stat='identity', width=1)
    # Format plot
    p0_format <- p0 +
      xlab("Number of Datasets")+
      ylab("Cost ($)")+
      guides(col = guide_legend(ncol = 3, byrow=FALSE)) +
      scale_fill_discrete(labels=c('Instance Cost   ', 'Storage Cost   ', 'Transfer Cost   ')) +
      scale_x_discrete(breaks=c(100, 2000, seq(0, max_num, 5000))) +
      theme_bw()+
      theme(axis.title.x = element_text(size=12,colour="black", vjust=-.8),
            axis.title.y = element_text(size=12,colour="black"),
            axis.text.x = element_text(size=8,colour="black", angle=35),
            axis.text.y = element_text(size=8,colour="black"),
            legend.position="bottom",
            legend.title=element_blank(),
            legend.key=element_blank(),
            legend.key.size=unit(1, "mm"),
            legend.margin=unit(1,"cm"),
            legend.key.height=unit(3,"mm"),
            legend.key.width=unit(3,"mm"),
            panel.margin=unit(20,"mm"),
            legend.text = element_text(size=10))

    # Add percentages (optional)
#     p0_perc <- p0_format + geom_text(data=subset(costs_maxnum, Num.datasets >= 7000),
#                                      aes(x=factor(Num.datasets), y=pos, label=label), size=2)

    # Add other line over the top (this isnt working, verrry hard to figure out)
    p0_withline <- p0_format + geom_line(data=costs_compare, aes(x=as.numeric(factor(num_datasets)), y=cap_costs))

    # Return plots
    return(p0_withline)
}


# Times dataframe and plots
times_df_plots <- function(df, max_num, times_compare){

    # Add total_time difference to df
    df$znodl_diff <- df$Total.time - df$Total.time.nodl

    # Reshape data
    times_df <- melt(df, id=c('X', 'Tranfer.cost', 'Storage.cost', 'Instance.cost', 'Wait.time',
                            'Total.cost', 'Total.time', 'Download.time', 'Upload.time', 'Run.time', 'Sim.index',
                            'Av.zone', 'Bid.ratio', 'Bid.price', 'Num.datasets',
                            'Start.time', 'Interrupts', 'First.Iter.Time'))

    # Get the levels for variable in required order
    times_df$variable <- factor(times_df$variable, levels=c('Total.time.nodl', 'znodl_diff'))
    times_df <- arrange(times_df, Num.datasets, variable)

    # Calculate the percentages
    #times_df <- ddply(times_df, .(Num.datasets), transform, percent=value/Total.time*100)

    # Format the labels and calculate their positions
    #times_df <- ddply(times_df, .(Num.datasets), transform, pos=(value - 0.5*value))
    times_df$label <- paste0(sprintf("%.0f", times_df$percent), "%")

    # Get all data under maxnum datasets
    times_maxnum <- subset(times_df, Num.datasets <= max_num)

    # Initial plot
    p0 <- ggplot() + geom_bar(data=times_maxnum,
                                aes(x=factor(Num.datasets), y=value/60.0, fill=variable), stat='identity')

    # Format plot
    p0_format <- p0 +
    xlab("Number of Datasets")+
    ylab("Time (hours)")+
    guides(col = guide_legend(ncol = 3, byrow = FALSE))+
    scale_fill_discrete(labels=c('No Download   ', 'Total Processing Time   ')) +
    scale_x_discrete(breaks=c(100, 2000, seq(0, max_num, 5000))) +
    theme_bw()+
    theme(axis.title.x = element_text(size=12,colour="black", vjust=-.8),
          axis.title.y = element_text(size=12,colour="black"),
          axis.text.x = element_text(size=8,colour="black", angle=35),
          axis.text.y = element_text(size=8,colour="black"),
          legend.position="bottom",
          legend.title=element_blank(),
          legend.key=element_blank(),
          legend.key.size=unit(1, "mm"),
          legend.margin=unit(1,"cm"),
          legend.key.height=unit(3,"mm"),
          legend.key.width=unit(3,"mm"),
          panel.margin=unit(20,"mm"),
          legend.text = element_text(size=10))

    # Add percentages (optional)
#     p0_perc <- p0_format +
#                geom_text(data=subset(times_maxnum, Num.datasets >=2000),
#                          aes(x=factor(Num.datasets), label=label, y=pos/60), position=position_dodge(width=1),
#                          size=3)

    # Add trendline
    p0_withline <- p0_format + geom_line(data=times_compare, aes(x=as.numeric(factor(num_datasets)), y=cap_time))

    # Return plots
    return(p0_withline)
}

# Fixed plots
cpac_csv <- '~/Documents/projects/Clark2015_AWS/spot-model/fixed-outs/cpac_with_dl.csv'
cpac_csv_nodl <- '~/Documents/projects/Clark2015_AWS/spot-model/fixed-outs/cpac_without_dl.csv'
fs_csv <- '~/Documents/projects/Clark2015_AWS/spot-model/fixed-outs/fs_with_dl.csv'
fs_csv_nodl <- '~/Documents/projects/Clark2015_AWS/spot-model/fixed-outs/fs_without_dl.csv'

# Init variables
jobs_per = 3
num_mins = 45
# Number of datasets
max_num = 50000

# Read in dataframe
df <- read.csv(fs_csv)
df_ndl <- read.csv(fs_csv_nodl)

# Create capital costs dataframe
costs_compare <- data.frame(num_datasets=df$Num.datasets[df$Num.datasets <= max_num])
times_compare <- data.frame(num_datasets=df$Num.datasets[df$Num.datasets <= max_num])

# Populate dateframe with capital costs
costs_compare$cap_costs <- apply(costs_compare[1], 1, jobs_per=jobs_per, num_mins=num_mins, calc_costs_cap)
times_compare$cap_time <- apply(times_compare[1], 1, jobs_per=jobs_per, num_mins=num_mins, calc_time_cap)

# Get plots
costs_plot <- costs_df_plots(df, max_num, costs_compare)
# Without percentages
print(costs_plot)

# Time no dl
df$Total.time.nodl <- df_ndl$Total.time
# Get plots
times_plot <- times_df_plots(df, max_num, times_compare)
# Without percentages
print(times_plot)

# Save pdfs
pdf(file = "~/Documents/projects/Clark2015_AWS/poster/fs-costs.pdf", width=5, height = 5)
print(costs_plot)
dev.off()

pdf(file = "~/Documents/projects/Clark2015_AWS/poster/fs-times.pdf", width=5, height = 5)
print(times_plot)
dev.off()
