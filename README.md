<a rel="Delivery" href="https://github.com/BCDevExchange/assets/blob/master/README.md"><img alt="In production, but maybe in Alpha or Beta. Intended to persist and be supported." style="border-width:0" src="https://assets.bcdevexchange.org/images/badges/delivery.svg" title="In production, but maybe in Alpha or Beta. Intended to persist and be supported." /></a>

# designatedlands

Combine conservation related spatial data from many sources to create a single 'Designated Lands' layer for British Columbia. Land designations that contribute to conservation are summarized in three broad categories: *Protected Lands* (further broken down into formal *Parks and Protected Areas*, and *Other Protected Lands*, *Resource Exclusion Areas* and *Spatially Managed Areas*.  Overlaps are removed such that areas with overlapping designations are assigned to the highest category.

A complete run of the tool was completed on Sept 21, 2017, and the results are reported on [Environmental Reporting BC](http://www.env.gov.bc.ca/soe/indicators/land/land-designations.html). 

## Requirements
- PostgreSQL 9.0+, PostGIS 2.0+ (tested with PostgreSQL 9.6.5, PostGIS 2.3.2)
- GDAL (with `ogr2ogr` available at the command line) (tested with GDAL 2.2.1_3)
- Python 2.7 (tested with 2.7.13)

## Optional 
- [mapshaper](https://github.com/mbloch/mapshaper) (for aggregating output data across tiles)

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
        
5. Clone the repository and install Python dependencies:
 
        $ git clone https://github.com/bcgov/designatedlands.git
        $ cd designatedlands
        $ pip install -r requirements.txt
    
    Note that this procedure for installing Python dependencies will likely not work for Windows users. On Windows, Fiona installation requires manually downloading the pre-built wheel. [See the Fiona manual for details and a link to the wheel](https://github.com/Toblerity/Fiona#windows). Once Fiona is manually installed, `pip install -r requirements.txt` should work to install the rest of the libraries. Some further PATH configurations will be required if you are installing Fiona to a Python installed by ArcGIS (not recommended).

6. Using the `pgxn` client (installed via `requirements.txt`, above), install the `lostgis` extension:

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

First, configure or adapt the file `sources.csv` as required. This file defines all layers/data sources to be processed and how the script will process each layer. See [below](#sources.csv) for a full description of this file and how it defines the various data sources.

For repeating the Sept 21, 2017 analysis, source BCGW data are provided in a zip file attached to the [latest release](https://github.com/bcgov/designatedlands/releases). Download that file and extract it to the `source_data` folder.

Next, download data sources specified as **manual downloads** in `sources.csv`.

Then, using the `designatedlands.py` tool, load and process all data and dump the results to geopackage:

```
$ python designatedlands.py create_db
$ python designatedlands.py load --email myemail@email.bc.ca
$ python designatedlands.py process
$ python designatedlands.py dump
```

See the `--help` for more options:
```
$ python designatedlands.py --help
Usage: designatedlands.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  create_db       Create a fresh database
  dump            Dump output designatedlands table to file
  dump_aggregate  Unsupported
  load            Download data, load to postgres
  overlay         Intersect layer with designatedlands
  process         Create output designatedlands tables
```

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
  --force_download TEXT  Force fresh download
  --help                 Show this message and exit.
```


#### Overlay
In addition to creating the output designated lands layer, this tool also provides a mechanism to overlay the results with administration or ecological units of your choice:

```
$ python designatedlands.py overlay --help
Usage: designatedlands.py overlay [OPTIONS] IN_FILE

  Intersect layer with designatedlands

Options:
  -dl, --dl_table TEXT         Name of output designated lands table
  -l, --in_layer TEXT          Input layer name
  --dump_file                  Dump to file (as specified by out_file and
                               out_format)
  -o, --out_file TEXT          Output geopackage name
  -of, --out_format TEXT       Output format. Default GPKG (Geopackage)
  -nln, --new_layer_name TEXT  Output layer name
  -p, --n_processes INTEGER    Number of parallel processing threads to
                               utilize
  --help                       Show this message and exit.
```

For example, to overlay `designatedlands` with BC ecosections, first get `ERC_ECOSECTIONS_SP.gdb` from [here](https://catalogue.data.gov.bc.ca/dataset/ecosections-ecoregion-ecosystem-classification-of-british-columbia), then run the following command to create output `dl_eco.gpkg/eco_overlay`: 

```
$ python designatedlands.py overlay ERC_ECOSECTIONS_SP.gdb \
    --in_layer WHSE_TERRESTRIAL_ECOLOGY_ERC_ECOSECTIONS_SP_polygon \
    --new_layer_name eco_overlay \
    --out_file dl_eco.gpkg \
    --out_format GPKG
```

### sources.csv
The file `sources.csv` defines all source layers and how they are processed. Edit this table to customize the analysis. Note that order of the rows is not important, the script will sort the rows by the **hierarchy** column. Columns are as follows:

| COLUMN                 | DESCRIPTION                                                                                                                                                                            | 
|------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| 
| **hierarchy**              | An integer defining the order in which to overlay layers. In areas where sources overlap the source with the higher hierarchy value will take precedence. Equivalent hierarchy values for different layers are valid. Sources required for processing but not included in the designated lands hierarchy (such as tiling, boundary, or preprocessing layers) should be give a hierarchy value of `0`. | 
| **exclude**              | A value of `T` will exclude the source from all operations | 
| **manual_download**        | A value of `T` indicates that a direct download url is not available for the data. Download these sources manually to the downloads folder and ensure that value given for **file_in_url** matches the name of the file in the download folder                                                            | 
| **name**                   | Full name of the designated land category                                                                                                                                                | 
| **alias**                  | A unique underscore separated value used for coding the various designated categories (eg `park_provincial`)                                                                                                |
| **designation_id_col**     | The column in the source data that defines the unique ID for each feature                                                                                                              |
| **designation_name_col**   | The column in the source data that defines the name for each feature                                                                                                                   |
| **category**                 | A number prefixed code defining the broader designated class to which the layer belongs. Leave blank for non designated lands sources (tiling, boundary or preprocessing layers)      | 
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