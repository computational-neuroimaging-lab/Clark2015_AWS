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
    - download_run_fs.py - script to download and run Freesurfer's 'recon-all' command on the CORR IBA_TRT data and then uploads the results to S3 on the 'fcp-indi' bucket
    - download_run_fs_gpu.py - script to download and run Freesurfer's 'recon-all' command using GPU-optimized binaries instead of CPU ones.
    - download_run_fs.sge - SGE bash script to launch download_run_fs.py over parallel HPC nodes via SGE

spot-model
----------
- S3_costs_2mm.R - R script that models and plots AWS costs for based on CPAC runtimes for 2mm images
- record_spot_price.py - python module to record the spot price history from AWS and save histories to pklz files and a csv dataframe
- spot_price_model.py - python module to simulate job submissions over spot history and calculate runtimes and costs
