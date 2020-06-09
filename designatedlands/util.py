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

import os
import hashlib
import requests
import shutil
import tarfile
import tempfile
import urllib.request
from urllib.parse import urlparse
import zipfile
from pathlib import Path
import logging
import pgdata

from osgeo import gdal
import fiona


CHUNK_SIZE = 1024
LOG = logging.getLogger(__name__)


def clip(db_url, in_table, clip_table, out_table):
    """Clip geometry of in_table by clip_table, writing output to out_table
    """
    db = pgdata.connect(db_url)
    columns = ", ".join(["a." + c for c in db[in_table].columns if c != "geom"])
    sql = f"""CREATE TABLE {out_table} AS
             SELECT
               {columns},
               CASE
                 WHEN ST_CoveredBy(a.geom, b.geom) THEN a.geom
                 ELSE ST_Multi(
                        ST_CollectionExtract(
                          ST_Intersection(a.geom,b.geom), 3)) END AS geom
             FROM {in_table} AS a
             INNER JOIN {clip_table} AS b
             ON ST_Intersects(a.geom, b.geom)
          """
    db.execute(sql)


def union(db_url, in_table, columns, out_table):
    """Union/merge overlapping records with equivalent values for provided columns
    """
    db = pgdata.connect(db_url)
    sql = f"""CREATE TABLE {out_table} AS
             SELECT
               {columns},
               (ST_Dump(ST_Union(geom))).geom as geom
             FROM {in_table}
             GROUP BY {columns}
          """
    db.execute(sql)


def create_rat(in_raster, lookup, band_number=1):
    """
    Create simple raster attribute table based on lookup {int: string} dict
    Output RAT columns: VALUE (integer), DESCRIPTION (string)
    eg: lookup = {1: "URBAN", 5: "WATER", 11: "AGRICULTURE", 16: "MINING"}
    https://gis.stackexchange.com/questions/333897/read-rat-raster-attribute-table-using-gdal-or-other-python-libraries
    """
    # open the raster at band
    raster = gdal.Open(in_raster, gdal.GA_Update)
    band = raster.GetRasterBand(band_number)

    # Create and populate the RAT
    rat = gdal.RasterAttributeTable()
    rat.CreateColumn("VALUE", gdal.GFT_Integer, gdal.GFU_Generic)
    rat.CreateColumn("DESCRIPTION", gdal.GFT_String, gdal.GFU_Generic)

    i = 0
    for value, description in sorted(lookup.items()):
        rat.SetValueAsInt(i, 0, int(value))
        rat.SetValueAsString(i, 1, str(description))
        i += 1

    raster.FlushCache()
    band.SetDefaultRAT(rat)
    raster = None
    rat = None
    band = None


def parallel_tiled(db_url, sql, tile, n_subs=1):
    """
    Create a connection and execute query for specified tile
    n_subs is the number of places in the sql query that should be
    substituted by the tile name
    """
    db = pgdata.connect(db_url, schema="designatedlands", multiprocessing=True)
    # As we are explicitly splitting up our job by tile and processing tiles
    # concurrently in individual connections we don't want the database to try
    # and manage parallel execution of these queries within these connections.
    # Turn off this connection's parallel execution:
    db.execute("SET max_parallel_workers_per_gather = 0")
    db.execute(sql, (tile + "%",) * n_subs)


def download_non_bcgw(url, path, filename, layer=None, overwrite=False):
    """
    Download and extract a zipfile to unique location
    Modified from https://github.com/OpenBounds/Processing/blob/master/utils.py
    """
    # create a unique name for downloading and unzipping, this ensures a given
    # url will only get downloaded once
    out_folder = os.path.join(path, hashlib.sha224(url.encode("utf-8")).hexdigest())
    out_file = os.path.join(out_folder, filename)
    if overwrite and os.path.exists(out_folder):
        shutil.rmtree(out_folder)
    if not os.path.exists(out_folder):
        LOG.info("Downloading " + url)
        parsed_url = urlparse(url)
        urlfile = parsed_url.path.split("/")[-1]
        _, extension = os.path.split(urlfile)
        fp = tempfile.NamedTemporaryFile("wb", suffix=extension, delete=False)
        if parsed_url.scheme == "http" or parsed_url.scheme == "https":
            res = requests.get(url, stream=True, verify=False)
            if not res.ok:
                raise IOError

            for chunk in res.iter_content(CHUNK_SIZE):
                fp.write(chunk)
        elif parsed_url.scheme == "ftp":
            download = urllib.request.urlopen(url)
            file_size_dl = 0
            block_sz = 8192
            while True:
                buffer = download.read(block_sz)
                if not buffer:
                    break

                file_size_dl += len(buffer)
                fp.write(buffer)
        fp.close()
        # extract zipfile
        Path(out_folder).mkdir(parents=True, exist_ok=True)
        LOG.info("Extracting %s to %s" % (fp.name, out_folder))
        zipped_file = get_compressed_file_wrapper(fp.name)
        zipped_file.extractall(out_folder)
        zipped_file.close()
    # get layer name
    if not layer:
        layer = fiona.listlayers(os.path.join(out_folder, filename))[0]
    return (out_file, layer)


class ZipCompatibleTarFile(tarfile.TarFile):
    """
    Wrapper around TarFile to make it more compatible with ZipFile
    Modified from https://github.com/OpenBounds/Processing/blob/master/utils.py
    """

    def infolist(self):
        members = self.getmembers()
        for m in members:
            m.filename = m.name
        return members

    def namelist(self):
        return self.getnames()


def get_compressed_file_wrapper(path):
    """ From https://github.com/OpenBounds/Processing/blob/master/utils.py
    """
    ARCHIVE_FORMAT_ZIP = "zip"
    ARCHIVE_FORMAT_TAR_GZ = "tar.gz"
    ARCHIVE_FORMAT_TAR_BZ2 = "tar.bz2"
    archive_format = None
    if path.endswith(".zip"):
        archive_format = ARCHIVE_FORMAT_ZIP
    elif path.endswith(".tar.gz") or path.endswith(".tgz"):
        archive_format = ARCHIVE_FORMAT_TAR_GZ
    elif path.endswith(".tar.bz2"):
        archive_format = ARCHIVE_FORMAT_TAR_BZ2
    else:
        try:
            with zipfile.ZipFile(path, "r") as f:
                archive_format = ARCHIVE_FORMAT_ZIP
        except:
            try:
                f = tarfile.TarFile.open(path, "r")
                f.close()
                archive_format = ARCHIVE_FORMAT_ZIP
            except:
                pass
    if archive_format is None:
        raise Exception("Unable to determine archive format")

    if archive_format == ARCHIVE_FORMAT_ZIP:
        return zipfile.ZipFile(path, "r")

    elif archive_format == ARCHIVE_FORMAT_TAR_GZ:
        return ZipCompatibleTarFile.open(path, "r:gz")

    elif archive_format == ARCHIVE_FORMAT_TAR_BZ2:
        return ZipCompatibleTarFile.open(path, "r:bz2")
