# spot-model/utils.py
#
# Author: Daniel Clark, 2015

'''
This module contains various utilities for the modules and scripts in
this folder or package
'''

# Setup log file
def setup_logger(logger_name, log_file, level, to_screen=False):
    '''
    Function to initialize and configure a logger that can write to file
    and (optionally) the screen.

    Parameters
    ----------
    logger_name : string
        name of the logger
    log_file : string
        file path to the log file on disk
    level : integer
        indicates the level at which the logger should log; this is
        controlled by integers that come with the python logging
        package. (e.g. logging.INFO=20, logging.DEBUG=10)
    to_screen : boolean (optional)
        flag to indicate whether to enable logging to the screen

    Returns
    -------
    logger : logging.Logger object
        Python logging.Logger object which is capable of logging run-
        time information about the program to file and/or screen
    '''

    # Import packages
    import logging

    # Init logger, formatter, filehandler, streamhandler
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s : %(message)s')

    # Write logs to file
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Write to screen, if desired
    if to_screen:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    # Return the logger
    return logger


