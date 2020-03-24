# Copyright 2017 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import configparser
import csv
import multiprocessing
import os
import logging
import sys

from designatedlands.config import defaultconfig


class ConfigError(Exception):
    """Configuration key error"""


class ConfigValueError(Exception):
    """Configuration value error"""


def log_config(verbose, quiet):
    verbosity = verbose - quiet
    log_level = max(10, 20 - 10 * verbosity)  # default to INFO log level
    logging.basicConfig(
        stream=sys.stderr,
        level=log_level,
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    )


def read_config(config_file):
    """Load and read provided configuration file
    """
    if config_file:
        if not os.path.exists(config_file):
            raise ConfigValueError(f"File {config_file} does not exist")
        config = configparser.ConfigParser()
        config.read(config_file)
        config_dict = dict(config["designatedlands"])
        # make sure output table is lowercase
        config_dict["out_table"] = config_dict["out_table"].lower()
        # convert n_processes to integer
        config_dict["n_processes"] = int(config_dict["n_processes"])
        # set default n_processes to the number of cores available minus one
        if config_dict["n_processes"] == -1:
            config_dict["n_processes"] = multiprocessing.cpu_count() - 1
        # don't try and use more cores than are available
        elif config_dict["n_processes"] > multiprocessing.cpu_count():
            config_dict["n_processes"] = multiprocessing.cpu_count()
    else:
        config_dict = defaultconfig.copy()
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
        source.update(
            (k, int(v)) for k, v in source.items() if k == "hierarchy" and v != ""
        )
        # for convenience, add the layer names to the dict
        hierarchy = str(source["hierarchy"]).zfill(2)
        input_table = "a" + hierarchy + "_" + source["alias"]
        tiled_table = "b" + hierarchy + "_" + source["alias"]
        cleaned_table = "c" + hierarchy + "_" + source["alias"]
        source.update(
            {
                "input_table": input_table,
                "cleaned_table": cleaned_table,
                "tiled_table": tiled_table,
            }
        )
    # return sorted list https://stackoverflow.com/questions/72899/
    return sorted(source_list, key=lambda k: k["hierarchy"])


def tidy_designations(db, sources, designation_key, out_table):
    """Add and populate 'category' column, tidy the national park designations
    """
    # add category (rollup) column by creating lookup table from source.csv
    lookup_data = [
        dict(alias=s[designation_key], category=s["category"])
        for s in sources
        if s["category"]
    ]
    # create lookup table
    db["category_lookup"].drop()
    db.execute(
        """CREATE TABLE category_lookup
                  (id SERIAL PRIMARY KEY, alias TEXT, category TEXT)"""
    )
    db["category_lookup"].insert(lookup_data)
    # add category column
    if "category" not in db[out_table].columns:
        db.execute(
            """ALTER TABLE {t}
                      ADD COLUMN category TEXT
                   """.format(
                t=out_table
            )
        )
    # populate category column from lookup
    db.execute(
        """UPDATE {t} AS o
                  SET category = lut.category
                  FROM category_lookup AS lut
                  WHERE o.designation = lut.alias
               """.format(
            t=out_table
        )
    )
    # Remove prefrixes and national park names from the designations tags
    sql = """UPDATE {t}
             SET designation = '01_park_national'
             WHERE designation LIKE '%%01_park_national%%';

             UPDATE {t}
             SET designation = substring(designation from 2)
             WHERE designation ~ '^[a-c][0-9]'
          """.format(
        t=out_table
    )
    db.execute(sql)
