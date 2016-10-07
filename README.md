# conservationlands

Combine conservation related spatial data from many sources to create a single 'Conservation Lands' layer for British Columbia.

# Installation

## Requirements
- PostgreSQL 8.4+, PostGIS 2.0+ (tested on PostgreSQL 9.4, PostGIS 2.2.2)
- ogr2ogr (GDAL)
- Python 2.7

See `requirements.txt` for additional Python package requirements.

## Installation
1. `pip install bcconservationlands`  

## Usage
The default db connection is `localhost:5432/postgis`

```
$ conservationlands download [download folder] [postgresql url]
$ conservationlands aggregate [download folder] [postgresql url]
```

## Development and testing

```
pip install -e bcconservationlands
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
