import os
import configparser
import multiprocessing


def read_config(config_file):
    """Load the configuration file
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    config_dict = config['designatedlands']
    if config_dict['n_processes'] == 0:
        config_dict['n_processes'] = multiprocessing.cpu_count() - 1
    if 'email' not in config_dict.keys():
        if os.environ["BCDATA_EMAIL"]:
            config_dict['email'] = os.environ["BCDATA_EMAIL"]
        else:
            raise ValueError('Provide an email in .cfg or set BCDATA_EMAIL')
    return config_dict
