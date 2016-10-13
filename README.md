# conservationlands

Combine conservation related spatial data from many sources to create a single 'Conservation Lands' layer for British Columbia.

## Requirements
- PostgreSQL 8.4+, PostGIS 2.0+ (tested on PostgreSQL 9.5, PostGIS 2.2.2)
- GDAL (with ogr2ogr available at the command line)
- Python 2.7
- [PhantomJS](http://phantomjs.org/download.html) (currently a requirement for scripted DataBC Catalog downloads via [bcdata](https://github.com/smnorris/bcdata))

## Installation
1. Install all requirements noted above

2. Ensure Python is available at the command prompt. If the path to your Python executable is not already included in your PATH environment variable you will probably have to add it, using a command something like this:  

        $ set PATH="E:\sw_nt\Python27\ArcGIS10.3";"E:\sw_nt\Python27\ArcGIS10.3\Scripts";%PATH%

3. Ensure `pip` is installed, [install](https://pip.pypa.io/en/stable/installing/) if it is not. 

4. (Optional) Consider installing the script to a [virtual environment](https://virtualenv.pypa.io/en/stable/) rather than to the system Python:

        
        $ pip install virtualenv                     # if not already installed
        $ mkdir conservationlands_venv
        $ virtualenv conservationlands_venv
        $ source conservationlands_venv/bin/activate # activate the env, posix
        $ conservationlands_venv\Scripts\activate    # activate the env, windows
        

5. On Windows, to install the Fiona dependency you will likely have to manually download the pre-built wheel. [See the Fiona manual for details and a link to the wheel](https://github.com/Toblerity/Fiona#windows)

6. Install `bcdata` and `pgdb` dependencies:  
If git is installed:

        pip install git+https://github.com/smnorris/bcdata#egg=bcdata
        pip install git+https://github.com/smnorris/pgdb#egg=pgdb

    If you don't have git installed to the command line, download the zipfiles from github, extract the archives, and install the packages with:

        cd bcdata-master
        pip install .
        cd ../pgdb-master
        pip install .

7. Finally, install the script itself. If git is available:
        
        pip install git+https://github.com/smnorris/conservationlands#egg=conservationlands

    Without git, download and extract the archive from github, then:

        cd conservationlands-master
        pip install .


## Usage


## Development and testing


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
