# conservationlands

Combine conservation related spatial data from many sources to create a single 'Conservation Lands' layer for British Columbia.

## Requirements
- PostgreSQL 8.4+, PostGIS 2.0+ (tested on PostgreSQL 9.5, PostGIS 2.2.2)
- GDAL (with ogr2ogr available at the command line)
- Python 2.7
- [PhantomJS](http://phantomjs.org/download.html) (currently a requirement for scripted DataBC Catalog downloads via [bcdata](https://github.com/smnorris/bcdata))
- git (optional, for less typing during installation)

## Installation
1. Install all requirements noted above

2. Ensure Python is available at the command prompt. If the path to your Python executable is not already included in your PATH environment variable you will probably have to add it, using a command something like this:  

        $ set PATH="E:\sw_nt\Python27\ArcGIS10.3";"E:\sw_nt\Python27\ArcGIS10.3\Scripts";%PATH%

3. Ensure `pip` is installed, [install](https://pip.pypa.io/en/stable/installing/) if it is not. 

4. (Optional) Consider installing dependencies to a [virtual environment](https://virtualenv.pypa.io/en/stable/) rather than to the system Python:

        
        $ pip install virtualenv                     # if not already installed
        $ mkdir conservationlands_venv
        $ virtualenv conservationlands_venv
        $ source conservationlands_venv/bin/activate # activate the env, posix
        $ conservationlands_venv\Scripts\activate    # activate the env, windows
        
5. If you have git, download the repository and install dependencies with:
 
        $ git clone https://github.com/smnorris/conservationlands.git
        $ cd conservationlands
        $ pip install -r requirements.txt
    
    If you don't have git, download and extract each of the non-pypi repositories manually (conservationlands, pgdb and bcdata), then install them and Fiona:

        $ cd pgdb-master
        $ pip install .
        $ cd ../bcdata-master
        $ pip install .
        $ pip install Fiona     # or via downloaded wheel on Windows, see below
        $ cd ../conservationlands-master

5. Note that on Windows, to install the Fiona dependency you will likely have to manually download the pre-built wheel. [See the Fiona manual for details and a link to the wheel](https://github.com/Toblerity/Fiona#windows). Some further PATH configurations will be required if you are installing Fiona to a Python installed by ArcGIS.

## Usage
The file `sources.csv` defines all layers/data sources to be processed and how the script will process each layer. 

Before running the script, manually download all files defined in `sources.csv` as **manual_download**=`T` to the `downloads` folder.

Once data are downloaded, script usage is:
```
Usage: conservationlands [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  clean                     Clean/validate all input data
  download                  Download data, load to postgres
  dump                      Dump output conservation lands layer to shp
  pre_process               Unsupported
  process                   Create output conservation lands layer
  load_manual_downloads     Load manually downloaded data to postgres
  run_all                   Run complete conservation lands job
```

For help regarding an individual command:
```
$ python conservationlands.py download --help
Usage: conservationlands download [OPTIONS]

  Download data, load to postgres

Options:
  -s, --source_csv PATH
  --email TEXT
  --dl_path PATH
  -a, --alias TEXT
  --help                 Show this message and exit.
```

### Examples
Presuming all manual downloads specified are complete, process all data, then dump the results to shapefile:
```
$ python conservationlands.py download --email myemail@email.bc.ca
$ python conservationlands.py load_manual_downloads
$ python conservationlands.py clean
$ python conservationlands.py process
$ python conservationlands.py dump
```
Or, run all the above steps in a single command:
```
$ python conservationlands.py run_all
```

Most commands allow the user to specify inputs other than the default. For example, to download a single layer with **alias**=`park_provincial` as defined in a file `newparks_sources.csv` to the folder `newparks_download`, and copy to postgres:
```
$ python conservationlands.py download \
  -a park_provincial \
  -s newparks_sources.csv \
  --email myemail@email.bc.ca \
  --dl_path newparks_download
```

### sources.csv
The file `sources.csv` defines all source layers and how they are processed. Edit this table to customize the analysis. Note that order of the rows is not important, the script will sort the rows by the `hierarchy` column. Columns are as follows:

| column                 | description                                                                                                                                                                            | 
|------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| 
| hierarchy              | An integer defining the importance of the conservation layer relative to other sources. In areas where sources overlap the source with the higher hierarchy value will take precedence. Layers with no hierarchy specified will not be included. Equivalent hierarchy values for different layers are valid. | 
| manual_download        | Tag as 'T' if a direct download url is not available for the data. Download these sources manually to the downloads folder and ensure that value given for `file_in_url` matches the name of the file in the download folder                                                            | 
| name                   | Full name of the conservation land category                                                                                                                                                | 
| alias                  | A Unique underscore separated value used for coding the various conservation category                                                                                                | 
| rollup                 | A numbered code defining the broader conservation class to which the layer belongs                                                                                                     | 
| url                    | Download url for the data source                                                                                                                                                       | 
| file_in_url            | Name of the file of interest in the download from specified url                                                                                                                        | 
| layer_in_file          | For downloads of multi-layer files - specify the layer of interest within the file                                                                                                     | 
| query                  | A SQL query defining the subset of data of interest from the given file/layer (SQLite dialect)                                                                                         | 
| metadata_url           | URL for metadata reference                                                                                                                                                             | 
| info_url               | Background/info url                                                                                                                                                                    | 
| preprocess_operation   | Not currently supported                                                                                                                                                                | 
| preprocess_layer_alias | Not currently supported                                                                                                                                                                | 
| notes                  | Misc notes related to layer                                                                                                                                                            | 

## Configuration
To modify the default files/folders/schemas used to hold the data, edit the CONFIG dictionary at the top of the script:
```
CONFIG = {"downloads": "downloads",
          "source_csv": "sources.csv",
          "out_table": "conservation_lands",
          "out_shp": "conservation_lands.shp",
          "schema": "conservation_lands"}
```

## License

    Copyright 2016 Province of British Columbia

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at 

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
