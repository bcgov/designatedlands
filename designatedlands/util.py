import csv
import os
import configparser
import multiprocessing


def read_config(config_file):
    """Load and read provided configuration file
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    config_dict = config['designatedlands']
    # make sure output table is lowercase to avoid quoting
    config_dict['out_table'] = config_dict['out_table'].lower()
    if config_dict['n_processes'] == '-1':
        config_dict['n_processes'] = multiprocessing.cpu_count() - 1
    if 'email' not in config_dict.keys():
        if os.environ["BCDATA_EMAIL"]:
            config_dict['email'] = os.environ["BCDATA_EMAIL"]
        else:
            raise ValueError('Provide an email in .cfg or set BCDATA_EMAIL')
    return config_dict


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
