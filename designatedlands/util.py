
import csv
import datetime
import logging
import os
import sys
import unicodedata

from designatedlands.config import config


def read_csv(path):
    """
    Load input csv file and return a list of dicts.
    - List is sorted by 'hierarchy' column
    - keys/columns added:
        + 'input_table'   - 'a'+hierarchy+'_'+src_table
        + 'tiled_table'   - 'b'+hierarchy+'_'+src_table
        + 'cleaned_table' - 'c'+hierarchy+'_'+src_table
    """
    source_list = [source for source in csv.DictReader(open(path))]
    for source in source_list:
        # convert hierarchy value to integer
        source.update((k, int(v)) for k, v in source.items()
                      if k == "hierarchy" and v != '')
        # for convenience, add the layer names to the dict
        hierarchy = str(source["hierarchy"]).zfill(2)
        input_table = "a"+hierarchy+"_"+source["alias"]
        tiled_table = "b"+hierarchy+"_"+source["alias"]
        cleaned_table = "c"+hierarchy+"_"+source["alias"]
        source.update({"input_table": input_table,
                       "cleaned_table": cleaned_table,
                       "tiled_table": tiled_table})
    # return sorted list https://stackoverflow.com/questions/72899/
    return sorted(source_list, key=lambda k: k['hierarchy'])


def make_sure_path_exists(path):
    """
    Make directories in path if they do not exist.
    Modified from http://stackoverflow.com/a/5032238/1377021
    """
    try:
        os.makedirs(path)
        return path
    except:
        pass


def log(message, level=None, name=None, filename=None):
    """
    Write a message to the log file and/or print to the the console.
    https://github.com/gboeing/osmnx/blob/master/osmnx/utils.py

    Parameters
    ----------
    message : string
        the content of the message to log
    Returns
    -------
    None
    """

    if level is None:
        level = config['log_level']
    if name is None:
        name = config['log_name']
    if filename is None:
        filename = config['log_filename']

    # if logging to file is turned on
    if config['log_file']:
        # get the current logger (or create a new one, if none), then log
        # message at requested level
        logger = get_logger(
            level=int(level),
            name=name,
            filename=filename,
            folder=config['logs_folder'])
        if level == logging.DEBUG:
            logger.debug(message)
        elif level == logging.INFO:
            logger.info(message)
        elif level == logging.WARNING:
            logger.warning(message)
        elif level == logging.ERROR:
            logger.error(message)

    # if logging to console is turned on, convert message to ascii and print to
    # the console
    if config['log_console']:
        # capture current stdout, then switch it to the console, print the
        # message, then switch back to what had been the stdout. this prevents
        # logging to notebook - instead, it goes to console
        standard_out = sys.stdout
        sys.stdout = sys.__stdout__

        # convert message to ascii for console display so it doesn't break
        # windows terminals
        message = unicodedata.normalize(
            'NFKD', str(message)).encode('ascii', errors='replace').decode()
        print(message)
        sys.stdout = standard_out


def get_logger(level, name, filename, folder):
    """
    Create a logger or return the current one if already instantiated.
    https://github.com/gboeing/osmnx/blob/master/osmnx/utils.py
    Parameters
    ----------
    level : int
        one of the logger.level constants
    name : string
        name of the logger
    filename : string
        name of the log file
    Returns
    -------
    logger.logger
    """

    logger = logging.getLogger(name)

    # if a logger with this name is not already set up
    if not getattr(logger, 'handler_set', None):

        # get today's date and construct a log filename
        todays_date = datetime.datetime.today().strftime('%Y_%m_%d')
        log_filename = os.path.join(
            folder, '{}_{}.log'.format(filename, todays_date))

        # if the logs folder does not already exist, create it
        make_sure_path_exists(folder)

        # create file handler and log formatter and set them up
        handler = logging.FileHandler(log_filename, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.handler_set = True

    return logger
