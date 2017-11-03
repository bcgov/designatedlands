# designatedlands

Combine conservation related spatial data from many sources to create a single 'Designated Lands' layer for British Columbia. Land designations that contribute to conservation are summarized in three broad categories: *Protected Lands* (further broken down into formal *Parks and Protected Areas*, and *Other Protected Lands*, *Resource Exclusion Areas* and *Spatially Managed Areas*.  Overlaps are removed such that areas with overlapping designations are assigned to the highest category.

## Requirements
- PostgreSQL 8.4+, PostGIS 2.0+ (tested on PostgreSQL 9.6.5, PostGIS 2.4)
- GDAL (with `ogr2ogr` available at the command line)
- Python 2.7

## Installation
1. Install the requirements noted above

2. Ensure Python is available at the command prompt. If the path to your Python executable is not already included in your PATH environment variable you will probably have to add it, using a command something like this:  

        $ set PATH="E:\sw_nt\Python27\ArcGIS10.3";"E:\sw_nt\Python27\ArcGIS10.3\Scripts";%PATH%

3. Ensure `pip` is installed, [install](https://pip.pypa.io/en/stable/installing/) if it is not. 

4. (Optional) Consider installing dependencies to a [virtual environment](https://virtualenv.pypa.io/en/stable/) rather than to the system Python:

        
        $ pip install virtualenv                   # if not already installed
        $ mkdir designatedlands_venv
        $ virtualenv designatedlands_venv
        $ source designatedlands_venv/bin/activate # activate the env, posix
        $ designatedlands_venv\Scripts\activate    # activate the env, windows
        
5. Clone the repository and install dependencies:
 
        $ git clone https://github.com/bcgov/designatedlands.git
        $ cd designatedlands
        $ pip install -r requirements.txt
    
    Note that this procedure for installing Python dependencies will likely not work for Windows users. On Windows, Fiona installation requires manually downloading the pre-built wheel. [See the Fiona manual for details and a link to the wheel](https://github.com/Toblerity/Fiona#windows). Once Fiona is manually installed, `pip install -r requirements.txt` should work to install the rest of the libraries. Some further PATH configurations will be required if you are installing Fiona to a Python installed by ArcGIS (not recommended).

6. Using the pgxn client (installed above), install the `lostgis` extension:
        $ pgxn install lostgis

## Configuration
To modify the default database/files/folders used to hold the data, edit the `CONFIG` dictionary at the top of `designatedlands.py`  

```
CONFIG = {
    "source_data": "source_data",
    "source_csv": "sources.csv",
    "out_table": "designatedlands",
    "out_file": "designatedlands.gpkg",
    "out_format": "GPKG",
    "db_url":
    "postgresql://postgres:postgres@localhost:5432/designatedlands",
    "n_processes": multiprocessing.cpu_count() - 1
    }
```

| KEY       | VALUE                                            |
|-----------|--------------------------------------------------| 
| `source_data`| path to folder that holds downloaded datasets |
| `source_csv`| path to file that holds all data source definitions |
| `out_table`| name of output table to create in postgres |
| `out_file` | Output geopackage name" |
| `out_format` | Output format. Default GPKG (Geopackage) |
| `db_url`| [SQLAlchemy connection URL](http://docs.sqlalchemy.org/en/latest/core/engines.html#postgresql) pointing to the postgres database
| `n_processes`| The inputs are broken up by tile and processed in parallel, define how many parallel processes to use. (default of -1 indicates number of cores on your machine minus one)|

## Usage
The file `sources.csv` defines all layers/data sources to be processed and how the script will process each layer. 

Before running the script, manually download all files defined in `sources.csv` as **manual_download**=`T` to the `source_data` folder.

Once data are downloaded, script usage is:
```
$ python designatedlands.py --help
Usage: designatedlands.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  create_db  Create an empty postgres db for processing
  dump       Dump output conservation lands layer to gdb
  load       Download data, load to postgres
  overlay    Intersect layer with designatedlands
  process    Create output conservation lands table
```
  <!-- run_all    Run complete conservation lands job -->

For help regarding an individual command:
```
$ python designatedlands.py load --help
Usage: designatedlands.py load [OPTIONS]

  Download data, load to postgres

Options:
  -s, --source_csv PATH  Path to csv that lists all input data sources
  --email TEXT           A valid email address, used for DataBC downloads
  --dl_path PATH         Path to folder holding downloaded data
  -a, --alias TEXT       The 'alias' key identifing the source of interest,
                         from source csv
  --help                 Show this message and exit.
```

### Usage

A complete run of the tool was completed on Sept 21, 2017, and the results are reported on [Environmental Reporting BC](http://www.env.gov.bc.ca/soe/indicators/land/land-designations.html). 

To preserve the source data from that analysis, and to avoid having to download all of the different layers from the BCGW, 
the source BCGW data are provided in a zip file attached to the [latest release](https://github.com/smnorris/conservationlands/releases). Download that file and extract it to the `source_data` folder.

Download the other data sources specified as **manual downloads** in `sources.csv`.

Then using the designatedlands tool, load and process all data, then dump the results to shapefile:
```
$ python designatedlands.py create_db
$ python designatedlands.py load --email myemail@email.bc.ca
$ python designatedlands.py process
$ python designatedlands.py dump
```
<!-- Or, run all the above steps in a single command:
```
$ python designatedlands.py run_all
```

Most commands allow the user to specify inputs other than the default. For example, to load a single layer with **alias**=`park_provincial` as defined in a file `newparks_sources.csv` to the folder `newparks_download`, and copy to postgres:
```
$ python designatedlands.py load \
  -a park_provincial \
  -s newparks_sources.csv \
  --email myemail@email.bc.ca \
  --dl_path newparks_download
```
-->

#### Overlay
In addition to creating the output conservation lands layer, this tool also provides a mechanism to overlay the results with administration or ecological units of your choice:

```
$ python designatedlands.py overlay --help
Usage: designatedlands.py overlay [OPTIONS] IN_FILE

  Intersect layer with designatedlands

Options:
  -l, --in_layer TEXT
  -o, --out_gdb TEXT           Name of output conservation lands geodatabase
  -nln, --new_layer_name TEXT  Output layer name
  --help
```

To overlay `designatedlands` with BC ecosections:
First get `ERC_ECOSECTIONS_SP.gdb` from [here](https://catalogue.data.gov.bc.ca/dataset/ecosections-ecoregion-ecosystem-classification-of-british-columbia)
```
# overlay with designatedlands layer to create output eco.gdb/ecosections_cnsrvtn
$ python designatedlands.py overlay ERC_ECOSECTIONS_SP.gdb --in_layer=WHSE_TERRESTRIAL_ECOLOGY_ERC_ECOSECTIONS_SP_polygon --new_layer_name=eco
# Dump the output to file
python designatedlands.py dump --out_table=eco_overlay --out_file=lands_eco.gpkg
```

### sources.csv
The file `sources.csv` defines all source layers and how they are processed. Edit this table to customize the analysis. Note that order of the rows is not important, the script will sort the rows by the **hierarchy** column. Columns are as follows:

| COLUMN                 | DESCRIPTION                                                                                                                                                                            | 
|------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| 
| **hierarchy**              | An integer defining the order in which to overlay layers. In areas where sources overlap the source with the higher hierarchy value will take precedence. Equivalent hierarchy values for different layers are valid. Sources required for processing but not included in the conservation lands hierarchy (such as tiling, boundary, or preprocessing layers) should be give a hierarchy value of `0`. | 
| **exclude**              | A value of `T` will exclude the source from all operations | 
| **manual_download**        | A value of `T` indicates that a direct download url is not available for the data. Download these sources manually to the downloads folder and ensure that value given for **file_in_url** matches the name of the file in the download folder                                                            | 
| **name**                   | Full name of the conservation land category                                                                                                                                                | 
| **alias**                  | A unique underscore separated value used for coding the various conservation categories (eg `park_provincial`)                                                                                                |
| **designation_id_col**     | The column in the source data that defines the unique ID for each feature                                                                                                              |
| **designation_name_col**   | The column in the source data that defines the name for each feature                                                                                                                   |
| **category**                 | A number prefixed code defining the broader conservation class to which the layer belongs. Leave blank for non conservation lands sources (tiling, boundary or preprocessing layers)      | 
| **url**                    | Download url for the data source                                                                                                                                                       | 
| **file_in_url**            | Name of the file of interest in the download from specified url. Omitted for BCGW downloads                                                                                            | 
| **layer_in_file**          | For downloads of multi-layer files, and BCGW object names - specify the layer of interest within the file                                                                              | 
| **query**                  | A SQL query defining the subset of data of interest from the given file/layer (SQLite dialect)                                                                                         | 
| **metadata_url**           | URL for metadata reference                                                                                                                                                             | 
| **info_url**               | Background/info url in addtion to metadata (if available)   | 
| **preprocess_operation**   | Pre-processing operation to apply to layer (`clip` is the only current supported operation)  | 
| **preprocess_layer_alias** | `alias` of an additional layer to use in the **preprocess_operation** (for example, to clip a source by the Muskwa-Kechika Management Area boundary, set **preprocess_operation** = `clip` and **preprocess_layer_alias** = `mk_boundary` | 
| **notes**                  | Misc notes related to layer                                                                                                                                                            | 
| **license**                | The license under which the data is distrubted.



## License

    Copyright 2017 Province of British Columbia

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at 

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

This repository is maintained by [Environmental Reporting BC](http://www2.gov.bc.ca/gov/content?id=FF80E0B985F245CEA62808414D78C41B). Click [here](https://github.com/bcgov/EnvReportBC-RepoList) for a complete list of our repositories on GitHub.