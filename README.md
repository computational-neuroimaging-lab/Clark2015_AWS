# Clark2015_AWS
Analyzing neuroimaging data in the AWS cloud

Contents
========

data-preproc
------------
- aws_pipelines.xlsx - Spreadsheet showing the pipeline runtime parameters on AWS for CPAC, Freesurfer (w/ and w/o GPU), ANTs, and QAP

    results
    -------
    - act_runtimes.png - histogram of the runtimes for completing ANTs cortical thickness on IBA_TRT subjects
    - act_runtimes.yml - YAML containing the runtimes in hours
    - adhd200_cpac_benchmark.results - summary of the runtime results for the CPAC run on subjects from the ADHD200 dataset
    - adhd200_cpac_runtimes.png - histogram of the runtime to complete running CPAC on subjects from the ADHD200 dataset
    - adhd200_cpac_runtimes.yml - YAML containing the runtimes in minutes
    - adhd200_fail_logs.yml - YAML containing s3 paths to the log files from the ADHD200 CPAC run
    - adhd200_outdirs_mb.png - historgram of the output directory sizes of the CPAC run on the ADHD200 subjects in MB
    - adhd200_outdirs_mb.yml - YAML of the directory sizes in MB
    - adhd200_upl_runtimes.png - histogram of the runtimes, in minutes, for output CPAC data to be uploaded to S3 from AWS EC2 instances
    - adhd200_upl_runtimes.yml - YAML of the runtimes in minutes
    - freesurfer_runtimes.png - histogram of the runtime to complete running 'recon-all' on all 50 of the subjects from IBA_TRT
    - fs_gpu_runtimes.png - histogram of runtimes for completing 'recon-all' using GPU optimizations on IBA_TRT
    - fs_gpu_runtimes.yml - YAML containing the runtimes in hours
    - fs_runtimes.yml - YAML containing the runtimes in hours

    scripts
    -------
    - act_interface.py - Nipype interface made to work with the ANTs cortical thickness extraction script found [here](https://raw.githubusercontent.com/stnava/ANTs/master/Scripts/antsCorticalThickness.sh)
    - act_run.py - Python script to run the ANTs cortical thickness script and upload results to S3
    - act_run.sge - SGE bash script to launch act_run.py over parallel HPC nodes via SGE
    - download_run_fs_gpu.py - script to download and run Freesurfer's 'recon-all' command using GPU-optimized binaries instead of CPU ones.
    - download_run_fs.py - script to download and run Freesurfer's 'recon-all' command on the CORR IBA_TRT data and then uploads the results to S3 on the 'fcp-indi' bucket
    - download_run_fs.sge - SGE bash script to launch download_run_fs.py over parallel HPC nodes via SGE
    - get_run_stats.py - Python script to pull data from pipeline log files and plot the runtime data

paper
-----
Transcript related to the AWS paper (now on Google docs)

poster
------
LaTeX files and images used to create the AWS/NDAR poster for OHBM 2015 and Neuroimformatics 2015

spot-model
----------
- record_spot_price.py - Python module to record the spot price history from AWS and save histories to csv dataframes and log files
- run_spot_sims.py - Python script to run AWS simulations over spot history in parallel using a configuration file and spot history csv
- run_static_model.py Python script to run the AWS static pricing model given a per-hour price, availability zone, and configuration file
- S3_costs_2mm.R - R script that models and plots AWS costs for based on CPAC runtimes for 2mm images
- spot_sim_plots.R - R script to create static model plots for the poster
- spot_sim_plots_Sw.R - R script to create static and simulation model plots for paper
- spot_price_model.py - Python module to simulate job submissions over spot history and calculate runtimes and costs
- utils.py - Python module with various utilities related to the AWS spot simulations, including dataframe consolidation and parallel processing

    configs
    -------
    ANTs, CPAC, and Freesurfer spot simulation config files with the runtime details for estimating time and cost of running on AWS

    csvs
    ----
    - ants_avg_simgs_and_static.csv - ANTs mean and median spot simulation averages along side static runtimes and costs
    - c3.8xlarge-allzones-avgs_03-15_09-04-2015.csv - Mean and median spot history price for the c3.8xlarge Linux/UNIX instance across availability zones from March 3 to Sept 4 2015
    - cpac_avg_simgs_and_static.csv - C-PAC mean and median spot simulation averages along side static runtimes and costs
    - fs_avg_simgs_and_static.csv - Freesurfer mean and median spot simulation averages along side static runtimes and costs

    plots
    -----
    Plots of the cost and times across number of datasets and bid ratios as well as mean simulation vs static costs and times
