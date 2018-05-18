<a rel="Delivery" href="https://github.com/BCDevExchange/assets/blob/master/README.md"><img alt="In production, but maybe in Alpha or Beta. Intended to persist and be supported." style="border-width:0" src="https://assets.bcdevexchange.org/images/badges/delivery.svg" title="In production, but maybe in Alpha or Beta. Intended to persist and be supported." /></a>

# designatedlands

Combine conservation related spatial data from many sources to create a single 'Designated Lands' layer for British Columbia. Land designations that contribute to conservation are summarized in three broad categories: *Protected Lands* (further broken down into formal *Parks and Protected Areas*, and *Other Protected Lands*, *Resource Exclusion Areas* and *Spatially Managed Areas*.  Overlaps are removed such that areas with overlapping designations are assigned to the highest category.

A complete run of the tool was completed on Sept 21, 2017, and the results are reported on [Environmental Reporting BC](http://www.env.gov.bc.ca/soe/indicators/land/land-designations.html). 

## Requirements
- PostgreSQL 10.0+, PostGIS 2.3+ (tested with PostgreSQL 10.2, PostGIS 2.4.3)
- GDAL (with `ogr2ogr` available at the command line) (tested with GDAL 2.2.3)
- Python 3.6
- Some systems may require tweaking of postgres and system kernel settings. An 
  example script to optimize on OSX is in `scripts/postgres_mac_setup.sh`.

## Optional 
- to aggregate output data across tiles, use [mapshaper](https://github.com/mbloch/mapshaper)

## Installation 
1. Install the requirements noted above. 

2. Clone the repository:
 
        $ git clone https://github.com/bcgov/designatedlands.git
        $ cd designatedlands

3. Ensure Python and `pip` (and optionally `pipenv`) are available at your command line (see [python-guide](http://docs.python-guide.org/en/latest/dev/virtualenvs/) for more info)  
  
4. Install [`lostgis`](https://github.com/gojuno/lostgis) PostreSQL functions:  

    **macos/linux**
        
        $ pip install pgxnclient
        $ pgxn install lostgis
            
    **Windows**  

    The [pgxn client](https://github.com/dvarrazzo/pgxnclient) does not work on Windows. See `scripts/lostgis_windows.bat` for a guide to installing the required functions. 

5. Install designatedlands package:        
     

     **macos/linux**
    
    Install to user's Python:

        $ pip install --user . 
        
    or install to a pipenv virtual environment: 
        
        $ pipenv install
        $ pipenv shell
        $ pip install .
        $ # If you are developing, install in 'editable' mode:
        $ # pip install -e .

     **Windows**  

     First, download the appropriate prebuilt wheel for Fiona following [this guide](https://github.com/Toblerity/Fiona#windows). The GDAL wheel may also be required. Install fiona using `pipenv`. Once fiona is installed, `pipenv install` should work to install other dependencies.
            

## Configuration
Modify general configuration of designatedlands by editing the default `designatedlands.cfg` or by creating your own config file with the same keys and passing it as an argument to the command line tool.

| KEY       | VALUE                                            |
|-----------|--------------------------------------------------| 
| `email`| Email address to use for downloading data from DataBC catalogue. Defaults to environment `BCDATA_EMAIL` if this is not provided
| `source_data`| path to folder that holds downloaded datasets |
| `source_csv`| path to file that holds all data source definitions |
| `out_table`| name of output table to create in postgres |
| `out_file` | Output geopackage name" |
| `out_format` | Output format. Default GPKG (Geopackage) |
| `db_url`| [SQLAlchemy connection URL](http://docs.sqlalchemy.org/en/latest/core/engines.html#postgresql) pointing to the postgres database
| `n_processes`| Input layers are broken up by tile and processed in parallel, define how many parallel processes to use. (default of -1 indicates number of cores on your machine minus one)|


## Usage

First, configure or adapt the file `sources.csv` as required. This file defines all layers/data sources to be processed and how the script will process each layer. See [below](#sources.csv) for a full description of this file and how it defines the various data sources.

If running a new analysis, download data sources specified as **manual downloads** in `sources.csv`.

Using the `designatedlands` tool, load and process all data then dump the results to geopackage:

```
$ designatedlands create_db
$ designatedlands load
$ designatedlands process
$ designatedlands dump
```

See the `--help` for more options:
```
$ designatedlands --help
Usage: designatedlands.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  create_db       Create a fresh database
  dump            Dump output designatedlands table to file
  load            Download data, load to postgres
  overlay         Intersect layer with designatedlands
  process         Create output designatedlands tables
```

For help regarding an individual command:
```
$ designatedlands load --help
Usage: designatedlands load [OPTIONS]

  Download data, load to postgres

Options:
  -a, --alias TEXT  The 'alias' key for the source of interest
  --force_download  Force fresh download
  --help            Show this message and exit.
```


#### Overlay
In addition to creating the output designated lands layer, this tool also provides a mechanism to overlay the results with administration or ecological units of your choice:

```
$ designatedlands overlay --help
Usage: designatedlands overlay [OPTIONS] IN_FILE

  Intersect layer with designatedlands

Options:
  -l, --in_layer TEXT          Input layer name
  --dump_file                  Dump to file (out_file in .cfg)
  -nln, --new_layer_name TEXT  Name of overlay output layer
  --help                       Show this message and exit.
```

For example, to overlay `designatedlands` with BC ecosections, first get `ERC_ECOSECTIONS_SP.gdb` from [here](https://catalogue.data.gov.bc.ca/dataset/ecosections-ecoregion-ecosystem-classification-of-british-columbia), then run the following command to create output `dl_eco.gpkg/eco_overlay`: 

```
$ designatedlands overlay \
    ERC_ECOSECTIONS_SP.gdb \
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
| **file_in_url**            | Name of the file of interest in the download from specified url. Not required for BCGW downloads.                                                                                            | 
| **layer_in_file**          | For downloads of multi-layer files. Not required for BCGW downloads     | 
| **query**                  | A SQL query defining the subset of data of interest from the given file/layer (SQLite dialect)                                                                                         | 
| **metadata_url**           | URL for metadata reference                                                                                                                                                             | 
| **info_url**               | Background/info url in addtion to metadata (if available)   | 
| **preprocess_operation**   | Pre-processing operation to apply to layer (`clip` and `union` are the only supported operations)  | 
| **preprocess_args** | Argument(s) to passs to **preprocess_operation** . `clip` requires a layer to clip by and `union` requires column(s) to aggregate by. For example, to clip a source by the Muskwa-Kechika Management Area boundary, set **preprocess_operation** = `clip` and **preprocess_args** = `mk_boundary` | 
| **notes**                  | Misc notes related to layer                                                                                                                                                            | 
| **license**                | The license under which the data is distrubted.

## Aggregate output layers with Mapshaper

As a part of data load, designatedlands dices all inputs into BCGS 1:20,000 map tiles. This speeds up processing significantly by enabling efficient parallel processing and limiting the size/complexity of input geometries. However, very small gaps are created between the tiles and re-aggregating (dissolving) output layers across tiles in PostGIS is error prone. While the gaps do not have any effect on the designated lands stats, they do need to be removed for display. Rather than attempt this in PostGIS, we can aggregate outputs using the topologically enabled [`mapshaper`](https://github.com/mbloch/mapshaper/) tool:

If not already installed, install node (<https://nodejs.org/en/>) and then 
install mapshaper with:

```
npm install -g mapshaper
```

```
# mapshaper doesn't read .gpkg, convert output to shp and use mapshaper 
# to snap and dissolve tiles
# requires mapshaper v0.4.72 to dissolve on >1 attribute
# use mapshaper-xl to allocate enough memory
ogr2ogr \
  designatedlands_tmp.shp \
  -sql "SELECT 
         designatedlands_id as dl_id, 
         designation as designat, 
         bc_boundary as bc_bound,
         category, 
         geom 
        FROM designatedlands" \
  designatedlands.gpkg \
  -lco ENCODING=UTF-8 &&
mapshaper-xl \
  designatedlands_tmp.shp snap \
  -dissolve designat,bc_bound \
    copy-fields=category \
  -explode \
  -o designatedlands_clean.shp &&
ls | grep -E "designatedlands_tmp\.(shp|shx|prj|dbf|cpg)" | xargs rm
```

Do the same for the overlaps file
```
ogr2ogr \
  designatedlands_overlaps_tmp.shp \
  -sql "SELECT 
         designatedlands_overlaps_id as dl_ol_id, 
         designation as designat,
         designation_id as des_id,
         designation_name as des_name,
         bc_boundary as bc_bound,
         category, 
         geom 
        FROM designatedlands_overlaps" \
  designatedlands.gpkg \
  -lco ENCODING=UTF-8 &&
mapshaper-xl \
  designatedlands_overlaps_tmp.shp snap \
  -dissolve designat,des_id,des_name,bc_bound \
    copy-fields=category \
  -explode \
  -o designatedlands_overlaps_clean.shp &&
ls | grep -E "designatedlands_overlaps_tmp\.(shp|shx|prj|dbf|cpg)" | xargs rm
```

## Results

The results of previous runs of the tool can be found on the [releases](https://github.com/bcgov/designatedlands/releases) page
of this repository.

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

## Credits

Thanks to [@Komzpa](https://github.com/Komzpa), [@pramsey](https://github.com/pramsey) and Stack Exchange for functions / blog posts / answers / advice on dealing with topological errors in PostGIS:

- [lostgis](https://github.com/gojuno/lostgis)
- [buffering](https://gis.stackexchange.com/questions/101639/postgis-st-buffer-breaks-because-of-no-forward-edges)
- [merging](https://gis.stackexchange.com/questions/31895/joining-lots-of-small-polygons-to-form-larger-polygon-using-postgis)
- [slivers](https://gis.stackexchange.com/questions/198115/how-to-delete-the-small-gaps-slivers-between-polygons-after-merging-adjacent-p)
- [prevent exceptions](http://www.tsusiatsoftware.net/jts/jtsfaq/jtsfaq.html#D9)