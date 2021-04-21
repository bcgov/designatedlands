[![img](https://img.shields.io/badge/Lifecycle-Stable-97ca00)](https://github.com/bcgov/repomountie/blob/master/doc/lifecycle-badges.md)[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


# designatedlands

Combine conservation related spatial data from many sources to create a single 'Designated Lands' layer for British Columbia. Land designations that contribute to conservation are summarized in three broad categories: *Protected Lands* (further broken down into formal *Parks and Protected Areas*, and *Other Protected Lands*, *Resource Exclusion Areas* and *Spatially Managed Areas*.  Overlaps are removed such that areas with overlapping designations are assigned to the highest category.

A complete run of the tool was completed on Sept 21, 2017, and the results are reported on [Environmental Reporting BC](http://www.env.gov.bc.ca/soe/indicators/land/land-designations.html).


## Requirements

- Python >=3.7
- GDAL (with `ogr2ogr` available at the command line) (tested with GDAL 3.0.2)
- a PostGIS enabled PostgreSQL database (tested with PostgreSQL 13, scripts require PostGIS >=3.1/Geos >=3.9)
- for the raster processing, a relatively large amount of RAM (tested with 64GB, should work with 32GB, 16GB is likely insufficent)

## Optional

- `conda` for managing Python requirements
- Docker for easy installation of PostgreSQL/PostGIS


## Installation (with conda and Docker)

This pattern should work on most OS.

1. Install Anaconda or [miniconda](https://docs.conda.io/en/latest/miniconda.html)

2. Open a [conda command prompt](https://docs.conda.io/projects/conda/en/latest/user-guide/getting-started.html)

3. Clone the repository and navigate to the project folder:

        $ git clone https://github.com/bcgov/designatedlands.git
        $ cd designatedlands

4. Create and activate a conda enviornment for the project using the supplied `environment.yml`:

        $ conda env create -f environment.yml
        $ conda activate designatedlands

5. Download and install Docker using the appropriate link for your OS:
    - [MacOS](https://download.docker.com/mac/stable/Docker.dmg)
    - [Windows](https://download.docker.com/win/stable/Docker%20Desktop%20Installer.exe)

6. Get a Postgres docker container with a PostGIS 3.1 / Geos 3.9 enabled database:

        $ docker pull postgis/postgis:13-master

7. Run the container, create the database, add required extensions (*note*: you will have to change the line continuation characters from `\` to `^` if running the job in Windows):

        $ docker run --name dlpg \
          -e POSTGRES_PASSWORD=postgres \
          -e POSTGRES_USER=postgres \
          -e PG_DATABASE=designatedlands \
          -p 5432:5432 \
          -d postgis/postgis:13-master
        $ psql -c "CREATE DATABASE designatedlands" postgres
        $ psql -c "CREATE EXTENSION postgis"
        $ psql -c "CREATE EXTENSION intarray"


    Running the container like this:

    - allows you to connect to it on port 5432 from localhost or 127.0.0.1
    - names the container dlpg

    Note that `designatedlands.py` uses the above database credentials as the default. If you need to change these (for example, changing the port
    to avoid conflicting with a system installation), modify the `db_url` parameter in the config file you supply to designatedlands (see below).

    As long as you don't remove this container, it will retain all the data you put in it. If you have shut down Docker or the container, you can start it up again with this command:

          $ docker start dlpg


## Usage

First, modify the `sources_designations.csv`and `sources_supporting.csv` files as required. These files define all designation data sources to be processed and how the script will process each source. See [below](#sources-csv-files) for a full description of these files and how they defines the various data sources.

If any data sources are specified as **manual downloads** in the source csv files, download the data to the `source_data` folder (or optionally to the folder identified by the `source_data` key in the config file)

Using the `designatedlands.py` command line tool, load and process all data then dump the results to .tif/geopackage:

```
$ python designatedlands.py download
$ python designatedlands.py preprocess
$ python designatedlands.py process-vector
$ python designatedlands.py process-raster
$ python designatedlands.py dump
```

See the `--help` for more options:
```
$ python designatedlands.py --help
Usage: designatedlands.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  cleanup          Remove temporary tables
  download         Download data, load to postgres
  dump             Dump output tables to file
  overlay          Intersect layer with designatedlands and write to GPKG
  preprocess       Create tiles layer and preprocess sources where required
  process-raster   Create raster designation/restriction layers
  process-vector   Create vector designation/restriction layers
  test-connection  Confirm that connection to postgres is successful
```

For help regarding an individual command:
```
$ python designatedlands.py download --help
Usage: designatedlands.py download [OPTIONS] [CONFIG_FILE]

  Download data, load to postgres

Options:
  -a, --alias TEXT  The 'alias' key for the source of interest
  --overwrite       Overwrite any existing output, force fresh download
  -v, --verbose     Increase verbosity.
  -q, --quiet       Decrease verbosity.
  --help            Show this message and exit.
```

## sources csv files

The files `sources_designations.csv` and `sources_supporting.csv` define all source layers and how they are processed. Edit these tables to customize the analysis.  Columns are noted below. All columns are present in `sources_designations.csv`, designation/hierarchy/restriction columns are not included in `sources_supporting.csv` but the remaining column definitions are identical. Note that order of rows in the files is not important, order your designations by populating the **hierarchy** column with integer values. Do not include a hierarchy integer for designations that are to be excluded (`exclude = T`)

| COLUMN                 | DESCRIPTION                                                                                                                                                                            |
|------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **hierarchy**              | An integer defining the order in which to overlay layers. In areas where sources overlap the source with the higher hierarchy value will take precedence. Equivalent hierarchy values for different layers are valid. Sources required for processing but not included in the designated lands hierarchy (such as tiling, boundary, or preprocessing layers) should be give a hierarchy value of `0`. |
| **exclude**              | A value of `T` will exclude the source from all operations |
| **manual_download**        | A value of `T` indicates that a direct download url is not available for the data. Download these sources manually to the downloads folder and ensure that value given for **file_in_url** matches the name of the file in the download folder                                                            |
| **name**                   | Full name of the designated land category                                                                                                                                                |
| **designation**                  | A unique underscore separated value used for coding the various designated categories (eg `park_provincial`)                                                                                                |
| **source_id_col**     | The column in the source data that defines the unique ID for each feature                                                                                                              |
| **source_name_col**   | The column in the source data that defines the name for each feature                                                                                                                   |
| **forest_restriction** | Level of restriction for the designation, forestry related activities (`Full`, `High`, `Medium`, `Low`, `None`)     |
| **og_restriction** | Level of restriction for the designation, oil and gas related activities (`Full`, `High`, `Medium`, `Low`, `None`)    |
| **mine_restriction** | Level of restriction for the designation, mine related activities (`Full`, `High`, `Medium`, `Low`, `None`)     |
| **url**                    | Download url for the data source                                                                                                                                                       |
| **file_in_url**            | Name of the file of interest in the download from specified url. Not required for BCGW downloads.                                                                                            |
| **layer_in_file**          | For downloads of multi-layer files. Not required for BCGW downloads     |
| **query**                  | A query defining the subset of data of interest from the given file/layer (CQL for BCGW sources, SQLITE dialect for other sources)                                                                                         |
| **metadata_url**           | URL for metadata reference                                                                                                                                                             |
| **info_url**               | Background/info url in addtion to metadata (if available)   |
| **preprocess_operation**   | Pre-processing operation to apply to layer (`clip` and `union` are the only supported operations)  |
| **preprocess_args** | Argument(s) to passs to **preprocess_operation** . `clip` requires a layer to clip by and `union` requires column(s) to aggregate by. For example, to clip a source by the Muskwa-Kechika Management Area boundary, set **preprocess_operation** = `clip` and **preprocess_args** = `mk_boundary` |
| **notes**                  | Misc notes related to layer                                                                                                                                                            |
| **license**                | The license under which the data is distrubted.


## Configuration

If required, you can modify the general configuration of designatedlands when running the commands above by supplying the path to a config file as a command line argument.
Note that the config file does not have to contain all parameters, you only need to include those where you do not wish to use the default values.

An example configuration file is included [`designateldands_sample_config.cfg`](designatedlands_sample_config.cfg), listing all available configuration parameters, setting the raster resolution to 25m, and using only 4 cores.

When using a configuration file, remember to specify it each time you use `designatedlands.py`, for example:

    $ python designatedlands.py download designatedlands_sample_config.cfg
    $ python designatedlands.py preprocess designatedlands_sample_config.cfg
    $ python designatedlands.py process-vector designatedlands_sample_config.cfg
    $ python designatedlands.py process-raster designatedlands_sample_config.cfg
    $ python designatedlands.py dump designatedlands_sample_config.cfg

| KEY       | VALUE                                            |
|-----------|--------------------------------------------------|
| `source_data`| path to folder that holds downloaded datasets |
| `sources_designations`| path to csv file holding designation data source definitions |
| `sources_supporting`| path to csv file holding supporting data source definitions |
| `out_path`| path to write output .gpkg and tiffs |
| `db_url`| [SQLAlchemy connection URL](http://docs.sqlalchemy.org/en/latest/core/engines.html#postgresql) pointing to the postgres database
| `resolution`| resolution of output geotiff rasters (m) |
| `n_processes`| Input layers are broken up by tile and processed in parallel, define how many parallel processes to use. (default of -1 indicates number of cores on your machine minus one)|




## Vector outputs

The `designatedlands.py dump` command writes two layers to output geopackage `outputs/designatedlands.gpkg`:

##### 1. `designations_overlapping`

Each individual designation polygon is clipped to the terrestrial boundary of BC, repaired if necessary, then loaded to this layer otherwise unaltered.
Where designations overlap, output polygons will overlap. Overlaps occur primarily between different designations, but are also present within the same designation.


##### 2. `designations_planarized`

Above `designations_overlapping` is further processed to remove overlaps and create a planarized output.
Where overlaps occur, they are noted in the attributes as semi-colon separated values. For example, a polygon where a `uwr_no_harvest` designation overlaps with a `land_act_reserves_17` designation will have values like this:

| designation | source_id | source_name | forest_restrictions | mine_restrictions | og_restrictions |
|-------------|-----------|-------------|-------------|-----------|-------------|
|`uwr_no_harvest;land_act_reserves_17`|`137810341;964007`|`u-3-005;SEC 17 DESIGNATED USE AREA`|`4;0` | `2;1` | `0;0`

The output restriction columns (`forest_restriction_max`,`mine_restriction_max`,`og_restriction_max`) are assigned the value of the highest restriction present within the polygon for the given restriction type.

Area totals for this layer are checked. To review the checks, see the tables in the postgres db:

- `qa_compare_outputs` - reports on total area of each designation and the difference between `designations_overlapping` and `designations_planarized`. Any differences should be due to same source overlaps.
- `qa_summary` - check that the total area of `designations_overlaps` matches total area of BC and check restriction areas.
- `qa_total_check` - check that the total for each restriction class adds up to the total area of BC


## Raster outputs

Four output rasters are created:

1. `designatedlands.tif` - output designations. In cases of overlap, the designation with the highest `process_order` is retained
2. `forest_restriction.tif` - output forest restriction levels
3. `mine_restriction.tif` - output mine restriction levels
4. `og_restriction.tif` - output oil and gas restriction levels

Raster attribute tables are available for each tif.


## Overlay

In addition to creating the output designated lands layer, this tool also provides a mechanism to overlay the results with administration or ecological units of your choice:

```
$ python designatedlands.py overlay --help
Usage: designatedlands.py overlay [OPTIONS] IN_FILE OUT_FILE [CONFIG_FILE]

  Intersect layer with designatedlands and write to GPKG

Options:
  -l, --in_layer TEXT     Name of input layer
  -nln, --out_layer TEXT  Name of output layer
  -v, --verbose           Increase verbosity.
  -q, --quiet             Decrease verbosity.
  --help                  Show this message and exit.
```

For example, to overlay `designatedlands` with BC ecosections, first get `ERC_ECOSECTIONS_SP.gdb` from [here](https://catalogue.data.gov.bc.ca/dataset/ecosections-ecoregion-ecosystem-classification-of-british-columbia), then run the following command to create output `dl_eco.gpkg/eco_overlay`:

```
$ python designatedlands.py overlay \
    ERC_ECOSECTIONS_SP.gdb \
    dl_eco.gpkg \
    --in_layer WHSE_TERRESTRIAL_ECOLOGY_ERC_ECOSECTIONS_SP \
    --out_layer eco_overlay
```

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
of this repository. The [`make_resources.sh`](scripts/make_resources.sh) script is used to generate the data hosted in the release.

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

Strategies for dealing with PostGIS precision issues:

- [lostgis](https://github.com/gojuno/lostgis)
- [buffering](https://gis.stackexchange.com/questions/101639/postgis-st-buffer-breaks-because-of-no-forward-edges)
- [merging](https://gis.stackexchange.com/questions/31895/joining-lots-of-small-polygons-to-form-larger-polygon-using-postgis)
- [slivers](https://gis.stackexchange.com/questions/198115/how-to-delete-the-small-gaps-slivers-between-polygons-after-merging-adjacent-p)
- [prevent exceptions](http://www.tsusiatsoftware.net/jts/jtsfaq/jtsfaq.html#D9)
