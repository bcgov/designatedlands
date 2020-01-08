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
import multiprocessing
import os


def read_config(config_file):
    """Load and read provided configuration file
    """
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
    if "email" not in config_dict.keys() or config_dict["email"] == "":
        if os.environ["BCDATA_EMAIL"]:
            config_dict["email"] = os.environ["BCDATA_EMAIL"]
        else:
            raise ValueError("Provide an email in .cfg or set $BCDATA_EMAIL")

    return config_dict


config = read_config("designatedlands.cfg")
