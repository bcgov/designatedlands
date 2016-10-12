# conservationlands

Combine conservation related spatial data from many sources to create a single 'Conservation Lands' layer for British Columbia.

## Requirements
- PostgreSQL 8.4+, PostGIS 2.0+ (tested on PostgreSQL 9.5, PostGIS 2.2.2)
- GDAL (ogr2ogr available at the command line)
- Python 2.7

## Installation
1. Install all requirements noted above. On Windows, you will also likely have to [manually install](https://github.com/Toblerity/Fiona#windows) `Fiona` (via the pre-compiled wheels binaries). 

2. Ensure Python is available at the command prompt. Setting your PATH environment to point to the Python executable may be required, something like this:  
```set PATH="E:\sw_nt\Python27\ArcGIS10.3";"E:\sw_nt\Python27\ArcGIS10.3\Scripts";%PATH%```
3. Ensure `pip` is installed, [install](https://pip.pypa.io/en/stable/installing/) if it is not. 

4. `pip install -e git+git://github.com/smnorris/conservationlands.git`

Pip should fetch all required python modules for you.

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
