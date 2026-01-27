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
import logging
import multiprocessing
from functools import partial
from xml.sax.saxutils import escape
import configparser
import os
import csv
from math import ceil
from urllib.parse import urlparse
import subprocess
from pathlib import Path
import hashlib
import requests
import shutil
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from datetime import date

import click
from cligj import verbose_opt, quiet_opt
from geoalchemy2 import Geometry
import rasterio
import pandas as pd
import numpy as np
from sqlalchemy.schema import Column
from sqlalchemy.types import Integer, UnicodeText
from affine import Affine
from osgeo import gdal
import fiona

import pgdata

import ftplib
import re
from urllib.parse import urlparse, urljoin

LOG = logging.getLogger(__name__)


DEFAULT_CONFIG = {
    "dl_path": "source_data",
    "sources_designations": "sources_designations.csv",
    "sources_supporting": "sources_supporting.csv",
    "out_path": "outputs",
    "db_url": "postgresql://postgres:postgres@localhost:5432/designatedlands",
    "n_processes": -1,
    "resolution": 10,
}


class ConfigError(Exception):
    """Configuration key error"""


class ConfigValueError(Exception):
    """Configuration value error"""


def set_log_level(verbose, quiet):
    verbosity = verbose - quiet
    log_level = max(10, 20 - 10 * verbosity)  # default to INFO log level
    logging.basicConfig(
        stream=sys.stderr,
        level=log_level,
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    )


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
    Download and extract a zipfile or download a remote folder to unique location.
    Handles:
      - archive URLs (zip / tar.gz / tar.bz2) : download -> extract -> locate datasource
      - FTP folder URLs (ending in / or without filename) : FTP-list -> download files into folder -> locate datasource
      - HTTP directory listing pages : parse links -> download matching files -> locate datasource
    Returns: (datasource_path, layer)
    """
    out_folder = os.path.join(path, hashlib.sha224(url.encode("utf-8")).hexdigest())
    if overwrite and os.path.exists(out_folder):
        shutil.rmtree(out_folder)
    Path(out_folder).mkdir(parents=True, exist_ok=True)

    parsed_url = urlparse(url)
    urlfile = os.path.basename(parsed_url.path) or ""
    _, extension = os.path.splitext(urlfile)
    
    # Check if URL contains archive filename in query parameters (e.g., path=.../file.zip)
    query_str = parsed_url.query or ""
    if not extension and "=" in query_str:
        # Extract filename from query parameters if it contains one
        for param in query_str.split("&"):
            if "=" in param:
                key, val = param.split("=", 1)
                # URL decode the value using urllib.parse.unquote
                from urllib.parse import unquote
                val_decoded = unquote(val)
                base = os.path.basename(val_decoded)
                _, ext = os.path.splitext(base)
                if ext in (".zip", ".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz"):
                    extension = ext
                    urlfile = base
                    break
    
    # treat URL as "directory" if path ends with '/' or no filename/extension present
    is_dir_url = parsed_url.path.endswith("/") or (extension == "" and not urlfile)

    target_file = None

    if is_dir_url:
        LOG.info("Detected directory URL; will download folder contents: %s" % url)
        # FTP directory: list and download all files in that directory
        if parsed_url.scheme == "ftp":
            ftp_host = parsed_url.hostname
            ftp_path = parsed_url.path or "/"
            LOG.info("Connecting to FTP %s, path: %s" % (ftp_host, ftp_path))
            ftp = ftplib.FTP(ftp_host)
            try:
                ftp.login()  # anonymous
                # change to target directory (strip leading '/')
                try:
                    ftp.cwd(ftp_path)
                except Exception:
                    # try chdir progressively
                    parts = [p for p in ftp_path.split("/") if p]
                    for p in parts:
                        ftp.cwd(p)
                entries = ftp.nlst()
                for entry in entries:
                    local_path = os.path.join(out_folder, os.path.basename(entry))
                    # skip existing
                    if os.path.exists(local_path):
                        continue
                    try:
                        with open(local_path, "wb") as fh:
                            ftp.retrbinary("RETR " + entry, fh.write)
                    except Exception:
                        # try retrieving with base name only
                        try:
                            with open(local_path, "wb") as fh:
                                ftp.retrbinary("RETR " + os.path.basename(entry), fh.write)
                        except Exception:
                            LOG.info("Failed to download FTP entry: %s" % entry)
                ftp.quit()
            except Exception as e:
                try:
                    ftp.quit()
                except Exception:
                    pass
                raise

        elif parsed_url.scheme in ("http", "https"):
            LOG.info("HTTP directory URL; attempting to parse listing and download known files from %s" % url)
            try:
                res = requests.get(url, verify=False, timeout=30)
                res.raise_for_status()
                html = res.text
                # find hrefs
                hrefs = re.findall(r'href=[\'"]?([^\'" >]+)', html, flags=re.IGNORECASE)
                # prefer files with these extensions
                wanted = (".zip", ".tar.gz", ".tgz", ".tar.bz2", ".shp", ".gpkg", ".geojson", ".json", ".kml")
                for href in hrefs:
                    # make absolute URL
                    file_url = urljoin(url, href)
                    if any(href.lower().endswith(ext) for ext in wanted):
                        local_name = os.path.basename(href)
                        local_path = os.path.join(out_folder, local_name)
                        if os.path.exists(local_path):
                            continue
                        try:
                            r2 = requests.get(file_url, stream=True, verify=False, timeout=60)
                            r2.raise_for_status()
                            with open(local_path, "wb") as fh:
                                for chunk in r2.iter_content(8192):
                                    fh.write(chunk)
                        except Exception:
                            LOG.info("Failed to download %s from %s" % (href, file_url))
                # if no matching files found, log and continue (later search may fail)
            except Exception as e:
                LOG.info("Unable to parse/download HTTP directory listing: %s" % conditionMessage(e) if 'conditionMessage' in globals() else str(e))

        else:
            raise Exception("Unsupported URL scheme for directory download: %s" % parsed_url.scheme)

        # After downloading files into out_folder, attempt to locate a datasource file there
        known_exts = {".shp", ".gpkg", ".sqlite", ".geojson", ".json", ".kml"}
        matches = []
        for root, dirs, files in os.walk(out_folder):
            for f in files:
                if os.path.splitext(f)[1].lower() in known_exts:
                    matches.append(os.path.join(root, f))
        if len(matches) == 1:
            target_file = matches[0]
        elif len(matches) > 1:
            # prefer gpkg, shp, geojson in that order
            for ext in (".gpkg", ".shp", ".geojson", ".json", ".kml"):
                for m in matches:
                    if m.lower().endswith(ext):
                        target_file = m
                        break
                if target_file:
                    break
        else:
            # if no direct datasource files, possibly shapefile components exist (.shp/.dbf/.shx)
            shp_candidates = []
            for root, dirs, files in os.walk(out_folder):
                for f in files:
                    if f.lower().endswith(".shp"):
                        shp_candidates.append(os.path.join(root, f))
            if len(shp_candidates) == 1:
                target_file = shp_candidates[0]
            elif len(shp_candidates) > 1:
                target_file = shp_candidates[0]

    else:
        # treat URL as a single file (likely an archive). Download to temp file with a sensible suffix
        LOG.info("Downloading file %s" % url)
        # determine sensible suffix for temp file (handle multi-part extensions)
        if urlfile.endswith(".tar.gz") or urlfile.endswith(".tgz"):
            suffix = ".tar.gz"
        elif urlfile.endswith(".tar.bz2") or urlfile.endswith(".tbz"):
            suffix = ".tar.bz2"
        else:
            suffix = os.path.splitext(urlfile)[1] or ""
        fp = tempfile.NamedTemporaryFile("wb", suffix=suffix, delete=False)
        if parsed_url.scheme in ("http", "https"):
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                    }
                    res = requests.get(
                        url,
                        stream=True,
                        verify=False,
                        timeout=60,
                        headers=headers
                    )
                    if not res.ok:
                        raise IOError(f"Download failed: {res.status_code} {res.reason}")
                    for chunk in res.iter_content(8192):
                        fp.write(chunk)
                    break  # success, exit retry loop
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # exponential backoff: 1s, 2s, 4s
                        LOG.warning(f"Connection error on attempt {attempt + 1}/{max_retries}: {e}. Retrying in {wait_time}s...")
                        import time; time.sleep(wait_time)
                    else:
                        raise IOError(f"Download failed after {max_retries} attempts: {e}")
        elif parsed_url.scheme == "ftp":
            download = urllib.request.urlopen(url)
            block_sz = 8192
            while True:
                buffer = download.read(block_sz)
                if not buffer:
                    break
                fp.write(buffer)
        else:
            raise Exception("Unsupported URL scheme: " + parsed_url.scheme)
        fp.close()

        LOG.info("Extracting %s to %s" % (fp.name, out_folder))
        zipped_file = get_compressed_file_wrapper(fp.name)
        zipped_file.extractall(out_folder)
        zipped_file.close()

        # locate the extracted datasource file (handle nested directories and shapefile component sets)
        candidate = os.path.join(out_folder, filename)
        if os.path.exists(candidate):
            target_file = candidate
        else:
            base_name, ext = os.path.splitext(filename)
            # walk extracted tree and try to find:
            for root, dirs, files in os.walk(out_folder):
                for f in files:
                    if f == filename or os.path.splitext(f)[0] == base_name:
                        target_file = os.path.join(root, f)
                        break
                if target_file:
                    break
            if target_file is None:
                known_exts = {".shp", ".gpkg", ".sqlite", ".geojson", ".json", ".kml"}
                matches = []
                for root, dirs, files in os.walk(out_folder):
                    for f in files:
                        if os.path.splitext(f)[1].lower() in known_exts:
                            matches.append(os.path.join(root, f))
                if len(matches) == 1:
                    target_file = matches[0]
                if target_file is None:
                    subdirs = [d for d in os.listdir(out_folder) if os.path.isdir(os.path.join(out_folder, d))]
                    if len(subdirs) == 1:
                        target_file = os.path.join(out_folder, subdirs[0])

    if target_file is None:
        raise Exception(f"Unable to locate datasource for requested file '{filename}' in {out_folder}")

    # get layer name (use fiona to list layers)
    if not layer:
        layer = fiona.listlayers(target_file)[0]
    return (target_file, layer)


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


class DesignatedLands(object):
    """ A class to hold the job's config, data and methods
    """

    def __init__(self, config_file=None):

        LOG.info("Initializing designatedlands")

        # load default config
        self.config = DEFAULT_CONFIG.copy()

        # if provided with a config file, replace config values with those present in
        # thie config file
        if config_file:
            if not os.path.exists(config_file):
                raise ConfigValueError(f"File {config_file} does not exist")
            self.read_config(config_file)

        # set default n_processes to the number of cores available minus one
        if self.config["n_processes"] == -1:
            self.config["n_processes"] = multiprocessing.cpu_count() - 1

        # don't try and use more cores than are available
        elif self.config["n_processes"] > multiprocessing.cpu_count():
            self.config["n_processes"] = multiprocessing.cpu_count()

        self.db = pgdata.connect(self.config["db_url"])
        self.db.ogr_string = f"PG:host={self.db.host} user={self.db.user} dbname={self.db.database} password={self.db.password} port={self.db.port}"

        # define valid restriction classes and assign raster values
        self.restriction_lookup = {
            "PROTECTED": 5,
            "FULL": 4,
            "HIGH": 3,
            "MEDIUM": 2,
            "LOW": 1,
            "NONE": 0,
        }
        # load sources from csv
        self.read_sources()

        # define bounds manually
        self.bounds = [273287.5, 367687.5, 1870687.5, 1735887.5]

        width = max(
            int(
                ceil(
                    (self.bounds[2] - self.bounds[0]) / float(self.config["resolution"])
                )
            ),
            1,
        )
        height = max(
            int(
                ceil(
                    (self.bounds[3] - self.bounds[1]) / float(self.config["resolution"])
                )
            ),
            1,
        )

        self.raster_profile = {
            "count": 1,
            "crs": "EPSG:3005",
            "width": width,
            "height": height,
            "transform": Affine(
                self.config["resolution"],
                0,
                self.bounds[0],
                0,
                -self.config["resolution"],
                self.bounds[3],
            ),
            "nodata": 255,
        }

    def read_config(self, config_file):
        """Load and read provided configuration file
        """
        config = configparser.ConfigParser()
        config.read(config_file)
        config_dict = dict(config["designatedlands"])
        # make sure output folder is lowercase
        if "out_path" in config_dict:
            config_dict["out_path"] = config_dict["out_path"].lower()
        # convert n_processes and resolution to integer
        if "n_processes" in config_dict:
            config_dict["n_processes"] = int(config_dict["n_processes"])
        if "resolution" in config_dict:
            config_dict["resolution"] = int(config_dict["resolution"])
        self.config.update(config_dict)

    def read_sources(self):
        """Load input csv files listing data sources
        """
        # load designations list and remove excluded rows
        designation_list = [
            s
            for s in csv.DictReader(open(self.config["sources_designations"]))
            if s["exclude"] != "T"
        ]

        # sort by process_order
        self.sources = sorted(designation_list, key=lambda k: int(k["process_order"]))

        # tidy strings
        for source in self.sources:
            for column in [
                "designation",
                "source_id_col",
                "source_name_col",
                "forest_restriction",
                "og_restriction",
                "mine_restriction",
            ]:
                source[column] = source[column].strip()

        # do some basic checks on the input csv to see if process_order and restriction classes make sense
        self.validate_sources()

        # create designation property, a list of dicts.
        # Initialize simply with {"process_order": n, "designation": val},
        self.designations = (
            pd.DataFrame(self.sources)
            .astype({"process_order": int})[["process_order", "designation"]]
            .drop_duplicates()
            .sort_values("process_order")
            .to_dict("records")
        )

        # add id column, convert process_order to filled string, strip other values
        for i, source in enumerate(self.sources, start=1):
            source["id"] = i
            # make sure there are no leading/trailing spaces introduced
            # (from editing source csv in excel)
            source["designation"] = source["designation"]
            source["source_id_col"] = source["source_id_col"]
            source["source_name_col"] = source["source_name_col"]
            source["forest_restriction"] = self.restriction_lookup[
                source["forest_restriction"].upper()
            ]
            source["og_restriction"] = self.restriction_lookup[
                source["og_restriction"].upper()
            ]
            source["mine_restriction"] = self.restriction_lookup[
                source["mine_restriction"].upper()
            ]

            source["process_order"] = str(source["process_order"]).zfill(2)
            source["src"] = (
                "src_" + str(source["id"]).zfill(2) + "_" + source["designation"]
            )
            source["preprc"] = source["src"] + "_preprc"
            source["dl"] = "dl_" + source["process_order"] + "_" + source["designation"]

        # read list of supporting layers and remove excluded rows
        supporting_list = [
            s for s in csv.DictReader(open(self.config["sources_supporting"]))
        ]

        # add id column
        for i, source in enumerate(supporting_list, start=(len(self.sources) + 1)):
            source["id"] = i
            source["process_order"] = "00"
            source["src"] = source["designation"]
        self.sources_supporting = supporting_list

        # load source csv to the db
        cmd = [
            "ogr2ogr.exe",
            "-overwrite",
            "-nlt",
            "NONE",
            "-nln",
            "sources",
            "-f",
            "PostgreSQL",
            "PG:host={h} port={p} user={u} dbname={db} password={pwd}".format(
                h=self.db.host,
                p=self.db.port,
                u=self.db.user,
                db=self.db.database,
                pwd=self.db.password,
            ),
            "-lco",
            "OVERWRITE=YES",
            self.config["sources_designations"],
        ]
        subprocess.run(cmd, shell=True)

    def validate_sources(self):
        """ Do some very basic validation of designations csv
        """
        # check that process_order numbers start at 1 and end at n designations
        h = list(set(int(d["process_order"]) for d in self.sources if d["exclude"] != "T"))
        if min(h) != 1:
            raise ValueError("Lowest process_order in source table must be 1")
        if min(h) + len(h) != max(h) + 1:
            raise ValueError(
                "Highest process_order value in source table must be equivalent "
                "to the number of unique (non-excluded) designations"
            )
        # check that restriction classes are valid, as per self.restriction_lookup
        for d in self.sources:
            if d["forest_restriction"].upper() not in self.restriction_lookup:
                raise ValueError(
                    "Invalid forest_restriction value {f} for source {d}".format(
                        f=d["forest_restriction"], d=d["designation"]
                    )
                )
            if d["og_restriction"].upper() not in self.restriction_lookup:
                raise ValueError(
                    "Invalid og_restriction value of {f} for source {d}".format(
                        f=d["forest_restriction"], d=d["designation"]
                    )
                )
            if d["mine_restriction"].upper() not in self.restriction_lookup:
                raise ValueError(
                    "Invalid mine_restriction value {f} for source {d}".format(
                        f=d["forest_restriction"], d=d["designation"]
                    )
                )

    def download(self, designation=None, overwrite=False):
        """Download source data
        """

        sources = self.sources_supporting + self.sources

        # if supplied a layer name, only process that layer
        if designation:
            sources = [s for s in sources if s["designation"] == designation]
            if not sources:
                raise ValueError("designation %s does not exist" % designation)

        # download and load everything that we can automate
        for source in [s for s in sources if s["manual_download"] != "T"]:
            # drop table if exists
            table_name = "public." + source["src"]
            if overwrite:
                self.db.execute(f"DROP TABLE IF EXISTS {table_name}")
            if table_name not in self.db.tables:
                # run BCGW downloads directly (bcdata has its own parallelization)
                if urlparse(source["url"]).hostname == "catalogue.data.gov.bc.ca":
                    # derive databc package name from the url
                    package = os.path.split(urlparse(source["url"]).path)[1]
                    cmd = [
                        "bcdata",
                        "bc2pg",
                        package,
                        "--db_url",
                        self.config["db_url"],
                        "--schema",
                        "public",
                        # be conservative, make just one request at a time
                        #"--max_workers",
                        #"1",
                        "--table",
                        source["src"],
                    ]
                    if source["query"]:
                        qry = source["query"]
                        # if query in sources has a {currdate} placeholder for 
                        # relative date queries, replace with the current date
                        currdate = date.today().isoformat()
                        qry = qry.format(currdate=currdate)
                        cmd = cmd + ["--query", qry]
                    LOG.info(" ".join(cmd))
                    subprocess.run(cmd)

                # run non-bcgw downloads
                else:
                    LOG.info("Loading " + source["src"])
                    file, layer = download_non_bcgw(
                        source["url"],
                        self.config["dl_path"],
                        source["file_in_url"],
                        source["layer_in_file"],
                        overwrite=overwrite,
                    )
                    # For peace_moberly, exclude shape_area field to avoid numeric overflow
                    select = None
                    if "peace_moberly" in source["src"].lower():
                        cmd = ["ogrinfo", "-so", file, layer]
                        result = subprocess.run(cmd, capture_output=True, text=True)
                        lines = result.stdout.split('\n')
                        fields = []
                        in_fields = False
                        for line in lines:
                            if 'Data axis to CRS axis mapping' in line:
                                in_fields = True
                            if in_fields and ':' in line and 'Real' in line:
                                field_name = line.split(':')[0].strip()
                                if field_name.lower() != 'shape_area':
                                    fields.append(field_name)
                        select = ",".join(fields)
                    
                    # Use ogr2ogr directly for more control over options
                    cmd = [
                        "ogr2ogr",
                        "-f", "PostgreSQL",
                        self.db.ogr_string,
                        file,
                        layer,
                        "-nln", f"public.{source['src']}",
                        "-lco", "SCHEMA=public"
                    ]
                    if source["query"]:
                        # If query is just a WHERE clause (doesn't start with SELECT), wrap it
                        query = source["query"]
                        if not query.strip().upper().startswith("SELECT"):
                            query = f"SELECT * FROM {layer} WHERE {query}"
                        cmd += ["-sql", query]
                    if select:
                        cmd += ["-select", select]
                    
                    # For GBR SFMA, add option to explode multipolygons into separate features
                    if "gbr_sfma" in source['src'].lower():
                        LOG.info(f"Handling MultiPolygon geometry for {source['src']} with -explodecollections")
                        cmd.insert(4, "-explodecollections")  # Insert after format
                    
                    LOG.info(" ".join(cmd))
                    subprocess.run(cmd)

            else:
                LOG.info(source["src"] + " already loaded.")

        # find and load manually downloaded sources
        for source in [s for s in sources if s["manual_download"] == "T"]:
            file = os.path.join(self.config["dl_path"], source["file_in_url"])
            if not os.path.exists(file):
                raise Exception(file + " does not exist, download it manually")
            table_name = "public." + source["src"]
            # drop table if exists
            if overwrite:
                self.db.execute(f"DROP TABLE IF EXISTS {table_name}")
            if table_name not in self.db.tables:
                self.db.ogr2pg(
                    file,
                    in_layer=source["layer_in_file"],
                    out_layer=source["src"],
                    sql=source["query"],
                )
            else:
                LOG.info(source["src"] + " already loaded.")

    def preprocess(self, designation=None):
        """
        Preprocess sources as specified
        Supported operations:
          - clip
          - union
        """
        # make sure safe overlay/repair functions are loaded
        self.db.execute(self.db.queries["ST_Safe_Repair"])
        self.db.execute(self.db.queries["ST_Safe_Difference"])
        self.db.execute(self.db.queries["ST_Safe_Intersection"])

        preprocess_sources = [
            s for s in self.sources if s["preprocess_operation"] != ""
        ]
        if designation:
            preprocess_sources = [
                s for s in preprocess_sources if s["designation"] == designation
            ]
        LOG.info("Preprocessing")
        for source in preprocess_sources:
            if source["preprocess_operation"] not in ["clip", "union"]:
                raise ValueError(
                    "Preprocess operation %s not supprted"
                    % source["preprocess_operation"]
                )
            t = source["preprc"]
            self.db.execute(f"DROP TABLE IF EXISTS {t}")
            # call the specified preprocess function
            if source["preprocess_operation"] == "clip":
                preprocess_table = "public." + source["preprocess_args"]
                if preprocess_table not in self.db.tables:
                    raise RuntimeError(
                        "Clip layer {l} not found. Ensure it is loaded".format(
                            l=source["preprocess_args"]
                        )
                    )
                LOG.info("Preprocessing " + source["src"])
                clip(
                    self.config["db_url"],
                    "public." + source["src"],
                    preprocess_table,
                    source["preprc"],
                )
            elif source["preprocess_operation"] == "union":
                LOG.info("Preprocessing " + source["src"])
                union(
                    self.config["db_url"],
                    source["src"],
                    source["preprocess_args"],
                    source["preprc"],
                )

    def create_bc_boundary(self):
        """
        Create a comprehensive and tiled land-marine layer.
        Combine these source layers (which must exist)

        - tiles_20k
        - tiles_250k
        - bc_boundary_land (BC boundary layer from GeoBC, does not include marine)
        - bc_abms (BC Boundary, ABMS)
        - marine_ecosections (BC Marine Ecosections)
        """
        db = self.db
        # create tiles table
        db.execute(db.queries["create_tiles"])

        # initialize empty land/marine definition table
        db.execute(
            """
            DROP TABLE IF EXISTS bc_boundary;
            CREATE TABLE bc_boundary (
                 bc_boundary_id serial PRIMARY KEY,
                 designation text,
                 map_tile text,
                 geom geometry(Polygon, 3005)
            );
            """
        )
        # Prep boundary sources
        # First, combine ABMS boundary and marine ecosections
        db.execute("DROP TABLE IF EXISTS bc_boundary_marine")
        db.execute(
            """
            CREATE TABLE bc_boundary_marine AS
                      SELECT
                        'bc_boundary_marine' as designation,
                         ST_Union(geom) as geom FROM
                          (SELECT st_union(geom) as geom
                           FROM bc_abms
                           UNION ALL
                           SELECT st_union(geom)::geometry(MULTIPOLYGON, 3005)::geometry(MULTIPOLYGON, 3005) as geom
                           FROM marine_ecosections) as foo
                       GROUP BY designation"""
        )
        # Create bc_boundary_land from bc_abms
        db.execute("DROP TABLE IF EXISTS bc_boundary_land")
        db.execute(
            """
            CREATE TABLE bc_boundary_land AS
            SELECT 'bc_boundary_land' as designation, geom FROM bc_abms
            """
        )
        for source in [
            "bc_boundary_land",
            "bc_boundary_marine",
        ]:
            LOG.info("Prepping and inserting into bc_boundary: %s" % source)
            # subdivide before attempting to tile
            db.execute(f"DROP TABLE IF EXISTS {source}_temp")
            db.execute(
                f"""
                CREATE UNLOGGED TABLE {source}_temp AS
                SELECT ST_Subdivide(geom) as geom FROM {source};
                CREATE INDEX ON {source}_temp USING GIST (geom);"""
            )

            # tile
            db.execute(f"DROP TABLE IF EXISTS {source}_tiled")
            lookup = {
                "src_table": f"{source}_temp",
                "out_table": f"{source}_tiled",
                "designation": source,
            }
            db.execute(db.build_query(db.queries["tile"], lookup))
            db.execute(f"DROP TABLE IF EXISTS public.{source}_temp")

            # combine the boundary layers into new table bc_boundary
            sql = self.db.build_query(
                self.db.queries["insert_difference"],
                {
                    "in_table": f"{source}_tiled",
                    "out_table": "bc_boundary",
                    "columns": "designation",
                    "query": "",
                    "source_pk": "id",
                },
            )
            tiles = self.get_tiles(f"{source}_tiled")
            func = partial(parallel_tiled, db.url, sql, n_subs=2)
            pool = multiprocessing.Pool(processes=self.config["n_processes"])
            pool.map(func, tiles)
            pool.close()
            pool.join()
        # rename the 'designation' column
        db.execute(
            """ALTER TABLE bc_boundary
                      RENAME COLUMN designation TO bc_boundary"""
        )
        # add index
        db.execute("CREATE INDEX ON bc_boundary USING GIST (geom)")

        # add empty restriction columns
        for restriction in ["forest", "og", "mine"]:
            db.execute(
                f"ALTER TABLE bc_boundary ADD COLUMN {restriction}_restriction integer;"
            )

    def get_geometry_column(self, table_name):
        """
        Detect the geometry column in a table using SQL.
        Queries PostgreSQL information schema to find geometry columns.
        
        Args:
            table_name: The name of the table to check
            
        Returns:
            The geometry column name, or None if not found
        """
        try:
            # Query PostgreSQL system catalog for geometry/geography columns
            sql = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND udt_name IN ('geometry', 'geography')
            LIMIT 1
            """
            result = self.db.execute(sql)
            rows = result.fetchall()
            if rows:
                geom_col = rows[0][0]
                LOG.debug(f"Detected geometry column '{geom_col}' in table {table_name}")
                return geom_col
        except Exception as e:
            LOG.debug(f"Could not detect geometry column for {table_name}: {e}")
        
        return None

    def create_designations_overlapping(self):
        """
        Create a single designatedlands table
        - holds all designations
        - terrestrial only
        - overlaps included
        """

        # create output table
        LOG.info("Creating designations_overlapping")
        sql = f"""
        DROP TABLE IF EXISTS designations_overlapping;
        CREATE TABLE designations_overlapping (
          designations_overlapping_id serial PRIMARY KEY,
          process_order integer,
          designation text,
          source_id text,
          source_name text,
          forest_restriction integer,
          og_restriction integer,
          mine_restriction integer,
          map_tile text,
          geom geometry(POLYGON, 3005)
        );
        """
        self.db.execute(sql)

        # insert data
        for source in self.sources:
            
            input_table = source["src"]
            if source["preprc"] in self.db.tables:
                input_table = source["preprc"]

            LOG.info(f"Inserting data from {input_table} into designations_overlapping")
            lookup = {
                "out_table": "designations_overlapping",
                "src_table": input_table,
                "process_order": str(int(source["process_order"])),
                "desig_type": source["designation"],
                "source_id_col": source["source_id_col"],
                "source_name_col": source["source_name_col"],
                "forest_restriction": str(source["forest_restriction"]),
                "og_restriction": str(source["og_restriction"]),
                "mine_restriction": str(source["mine_restriction"]),
            }
            try:
                sql = self.db.build_query(
                    self.db.queries["create_designations_overlapping"], lookup
                )
                
                # Handle geometry column variations for different sources
                src_lower = source["src"].lower()
                
                # Detect actual geometry column in the source table
                geom_col = self.get_geometry_column(input_table)
                if geom_col and geom_col != 'geom':
                    # Replace geometry column references if it's not the standard 'geom'
                    sql = sql.replace("a.geom", f"a.{geom_col}")
                
                # Note: bc_boundary table has its 'designation' column renamed to 'bc_boundary'
                # so the WHERE clause should reference 'bc_boundary' not 'designation'
                
                # Handle specific sources with known column naming issues
                # These sources have non-standard SRIDs that need transformation to 3005
                import re
                
                # For flathead, since it has a non-standard SRID, ensure transformation to 3005
                if "flathead" in src_lower:
                    # Flathead has SRID 900915 and uses wkb_geometry column
                    sql = re.sub(r'a\.wkb_geometry', 'ST_Transform(a.wkb_geometry, 3005)', sql)
                    LOG.debug(f"Applied Flathead SRID transformation for {input_table}")
                
                elif "national_wildlife_area" in src_lower or "migratory_bird_sanctuary" in src_lower:
                    # Canadian Protected Areas (processes 10, 12): shape column in SRID 102001 needs transformation to 3005
                    # Must transform ALL references to a.shape to avoid SRID mismatch errors
                    sql = re.sub(r'ST_Intersects\(a\.shape,', 'ST_Intersects(ST_Transform(a.shape, 3005),', sql)
                    sql = re.sub(r'(ST_CoveredBy|ST_Intersection)\(a\.shape,', r'\1(ST_Transform(a.shape, 3005),', sql)
                    # Also transform the result in THEN clause: "THEN a.shape" -> "THEN ST_Transform(a.shape, 3005)"
                    sql = re.sub(r'THEN a\.shape\b', 'THEN ST_Transform(a.shape, 3005)', sql)
                    LOG.debug(f"Applied Canadian Protected Areas SRID transformation (102001 -> 3005) for {input_table}")
                
                elif "gbr_sfma" in src_lower:
                    # GBR SFMA uses wkb_geometry column with SRID 900914 needs transformation to 3005
                    sql = re.sub(r'a\.wkb_geometry', 'ST_Transform(a.wkb_geometry, 3005)', sql)
                    LOG.debug(f"Applied GBR SFMA SRID transformation for {input_table}")
                
                elif "great_bear_grizzly" in src_lower or "great_bear_ebm" in src_lower or "great_bear_fisheries" in src_lower:
                    # Great Bear GDB sources: shape column in SRID 900914/900916 needs transformation to 3005
                    # Must transform ALL references to a.shape to avoid SRID mismatch errors
                    sql = re.sub(r'ST_Intersects\(a\.shape,', 'ST_Intersects(ST_Transform(a.shape, 3005),', sql)
                    sql = re.sub(r'(ST_CoveredBy|ST_Intersection)\(a\.shape,', r'\1(ST_Transform(a.shape, 3005),', sql)
                    # Also transform the result in THEN clause: "THEN a.shape" -> "THEN ST_Transform(a.shape, 3005)"
                    sql = re.sub(r'THEN a\.shape\b', 'THEN ST_Transform(a.shape, 3005)', sql)
                    LOG.debug(f"Applied Great Bear GDB SRID transformation for {input_table}")
                    
                elif "boreal_caribou" in src_lower:
                    # Boreal Caribou RRA: transform SRID (wkb_geometry column with SRID 900916 needs transformation to 3005)
                    sql = re.sub(r'a\.wkb_geometry', 'ST_Transform(a.wkb_geometry, 3005)', sql)
                    LOG.debug(f"Applied Boreal Caribou SRID transformation for {input_table}")
                    
                elif "peace_moberly" in src_lower:
                    # Peace-Moberly: transform SRID (wkb_geometry column with SRID 900916 needs transformation to 3005)
                    sql = re.sub(r'a\.wkb_geometry', 'ST_Transform(a.wkb_geometry, 3005)', sql)
                    LOG.debug(f"Applied Peace-Moberly SRID transformation for {input_table}")
                
                self.db.execute(sql)
                LOG.info(f"Successfully inserted data from {input_table} into designations_overlapping")
                
            except Exception as e:
                LOG.error(f"Error processing {input_table} for {source['src']}: {e}")
                # Log the error but continue with next source
                continue
        self.db.execute("CREATE INDEX ON designations_overlapping USING GIST (geom)")

    def create_designations_planarized(self):
        """
        From designations_overlapping, create designatedlands table with no overlaps
        - holds all designations
        - terrestrial only
        - planarize features and aggregate overlap data into arrays
        """

        # create output table

        self.db.execute("DROP TABLE IF EXISTS create_designations_planarized")
        LOG.info("Creating designations_planarized")
        sql = f"""
            DROP TABLE IF EXISTS designations_planarized;
            CREATE TABLE designations_planarized (
              designations_planarized_id serial primary key,
              process_order integer[],
              designation text[],
              source_id text[],
              source_name text[],
              forest_restrictions integer[],
              mine_restrictions integer[],
              og_restrictions integer[],
              forest_restriction_max integer,
              mine_restriction_max integer,
              og_restriction_max integer,
              map_tile text,
              geom geometry(POLYGON, 3005)
            );
        """
        self.db.execute(sql)

        # insert data
        LOG.info(f"Inserting data into designations_planarized")
        sql = self.db.queries["create_designations_planarized"]
        tiles = self.get_tiles("bc_boundary_land_tiled")
        func = partial(parallel_tiled, self.db.url, sql, n_subs=2)
        pool = multiprocessing.Pool(processes=self.config["n_processes"])
        # add a progress bar
        results_iter = pool.imap_unordered(func, tiles)
        with click.progressbar(results_iter, length=len(tiles)) as bar:
            for _ in bar:
                pass
        pool.close()
        pool.join()

        # index geom
        self.db["public.designations_planarized"].create_index_geom()

        # qa the outputs
        self.db.execute(self.db.queries["qa"])

    def rasterize(self):
        """
        Dump all designatinons to raster
        We use gdal_rasterize because:
        - easy (processing rasterio in parallel requires additional code)
        - handy to have the temp rasters written to disk in case of problems
        """
        # create temp raster folder
        Path("rasters").mkdir(parents=True, exist_ok=True)
        # build gdal_rasterize command
        # Note - do not create a tiled tiff (-co TILED=YES)
        # This option requires setting the GDAL_CACHEMAX to avoid hitting a
        # gdal bug https://github.com/OSGeo/gdal/issues/2261), and when setting
        # the cache to just under 2G, the process is far slower than writing to
        # a stripped tif
        gdal_rasterize = [
            "gdal_rasterize",
            "-a_nodata",
            "255",
            "-co",
            "COMPRESS=DEFLATE",
            "-co",
            "NUM_THREADS=ALL_CPUS",
            "-ot",
            "Byte",
            "-tr",
            str(self.config["resolution"]),
            str(self.config["resolution"]),
            "-te",
            str(self.bounds[0]),
            str(self.bounds[1]),
            str(self.bounds[2]),
            str(self.bounds[3]),
            self.db.ogr_string,
        ]
        # first, rasterize bc boundary
        query = "SELECT * FROM bc_boundary_land"
        process_order = 0
        command = gdal_rasterize + [
            "-burn",
            f"{process_order}",
            "-sql",
            f"{query}",
            f"rasters/dl_{process_order}.tif",
        ]
        LOG.info(" ".join(command))
        subprocess.run(command)
        # then rasterize the rest
        for process_order in reversed(
            list(set([int(s["process_order"]) for s in self.sources]))
        ):
            query = f"SELECT * FROM designations_overlapping WHERE process_order={process_order}"
            command = gdal_rasterize + [
                "-burn",
                f"{process_order}",
                "-sql",
                f"{query}",
                f"rasters/dl_{process_order}.tif",
            ]
            LOG.info(" ".join(command))
            subprocess.run(command)

    def overlay_rasters(self):
        """Overlay raster designations to remove overlaps using chunk-based processing
        Prioritize memory efficiency over I/O: process one chunk at a time, write immediately
        """
        import gc
        
        LOG.info("Overlaying rasters")
        
        # Get raster dimensions
        with rasterio.open("rasters/dl_0.tif") as src:
            height = src.height
            width = src.width
            profile = src.profile.copy()
        
        # Define chunk size - smaller chunks for memory efficiency
        chunk_height = 5000  # Process 5000 rows at a time (~745 MB per uint8 array)
        
        LOG.info(f"- processing {height}x{width} raster in chunks of {chunk_height} rows")
        
        # Get list of sources to process
        sources_list = sorted(
            list(
                set(
                    [
                        (
                            int(s["process_order"]),
                            int(s["forest_restriction"]),
                            int(s["og_restriction"]),
                            int(s["mine_restriction"]),
                        )
                        for s in self.sources
                    ]
                )
            ),
            key=lambda x: (-x[0]),
        )
        
        # Create output files for writing
        Path(self.config["out_path"]).mkdir(parents=True, exist_ok=True)
        
        output_files = {
            "designatedlands": rasterio.open(
                os.path.join(self.config["out_path"], "designatedlands.tif"),
                "w",
                driver="GTiff",
                dtype="uint8",
                count=1,
                width=width,
                height=height,
                crs="EPSG:3005",
                transform=profile["transform"],
                nodata=255,
                compress="deflate",
            ),
            "forest_restriction": rasterio.open(
                os.path.join(self.config["out_path"], "forest_restriction.tif"),
                "w",
                driver="GTiff",
                dtype="uint8",
                count=1,
                width=width,
                height=height,
                crs="EPSG:3005",
                transform=profile["transform"],
                nodata=255,
                compress="deflate",
            ),
            "og_restriction": rasterio.open(
                os.path.join(self.config["out_path"], "og_restriction.tif"),
                "w",
                driver="GTiff",
                dtype="uint8",
                count=1,
                width=width,
                height=height,
                crs="EPSG:3005",
                transform=profile["transform"],
                nodata=255,
                compress="deflate",
            ),
            "mine_restriction": rasterio.open(
                os.path.join(self.config["out_path"], "mine_restriction.tif"),
                "w",
                driver="GTiff",
                dtype="uint8",
                count=1,
                width=width,
                height=height,
                crs="EPSG:3005",
                transform=profile["transform"],
                nodata=255,
                compress="deflate",
            ),
        }
        
        # Pre-calculate chunk ranges
        chunk_ranges = [(i, min(i + chunk_height, height)) for i in range(0, height, chunk_height)]
        total_chunks = len(chunk_ranges)
        
        try:
            # MEMORY-OPTIMIZED: Process one chunk at a time, write immediately, then discard
            # Chunks outer loop, sources inner loop
            for chunk_idx, (chunk_start, chunk_end) in enumerate(chunk_ranges):
                chunk_rows = chunk_end - chunk_start
                
                LOG.info(f"- processing chunk {chunk_idx + 1}/{total_chunks} (rows {chunk_start}-{chunk_end})")
                
                # Initialize chunk arrays from BC boundary (dl_0)
                with rasterio.open("rasters/dl_0.tif") as src:
                    bc_chunk = src.read(1, window=rasterio.windows.Window(0, chunk_start, width, chunk_rows)).astype('uint8')
                
                designation_chunk = bc_chunk.copy()
                # Initialize restriction chunks to 0 (NONE) instead of copying BC boundary values
                # Restrictions and designations are different - restrictions should start at NONE
                forest_chunk = np.zeros_like(bc_chunk)
                og_chunk = np.zeros_like(bc_chunk)
                mine_chunk = np.zeros_like(bc_chunk)
                del bc_chunk
                
                # Process all sources for this chunk
                for source_idx, source in enumerate(sources_list):
                    (
                        process_order_val,
                        forest_restriction_val,
                        og_restriction_val,
                        mine_restriction_val,
                    ) = source
                    
                    # Open source raster, read this chunk, then close
                    with rasterio.open(f"rasters/dl_{process_order_val}.tif") as src:
                        B_chunk = src.read(1, window=rasterio.windows.Window(0, chunk_start, width, chunk_rows)).astype('uint8')
                    
                    # Update cells where B_chunk has this source's value
                    mask = B_chunk == process_order_val
                    
                    # Update designation for matching cells
                    designation_chunk[mask] = process_order_val
                    
                    # Update restrictions only where they should be more restrictive
                    if forest_restriction_val > 0:
                        forest_chunk[mask & (forest_chunk < forest_restriction_val)] = forest_restriction_val
                    if og_restriction_val > 0:
                        og_chunk[mask & (og_chunk < og_restriction_val)] = og_restriction_val
                    if mine_restriction_val > 0:
                        mine_chunk[mask & (mine_chunk < mine_restriction_val)] = mine_restriction_val
                    
                    del B_chunk, mask
                
                # Write this chunk to output files
                output_files["designatedlands"].write(designation_chunk, indexes=1, window=rasterio.windows.Window(0, chunk_start, width, chunk_rows))
                output_files["forest_restriction"].write(forest_chunk, indexes=1, window=rasterio.windows.Window(0, chunk_start, width, chunk_rows))
                output_files["og_restriction"].write(og_chunk, indexes=1, window=rasterio.windows.Window(0, chunk_start, width, chunk_rows))
                output_files["mine_restriction"].write(mine_chunk, indexes=1, window=rasterio.windows.Window(0, chunk_start, width, chunk_rows))
                
                # Clean up chunk arrays immediately
                del designation_chunk, forest_chunk, og_chunk, mine_chunk
                gc.collect()
                LOG.info(f"- chunk {chunk_idx + 1}/{total_chunks} completed and memory freed")
            
            LOG.info("- overlay processing completed successfully")
        
        finally:
            # Close all output files
            for f in output_files.values():
                f.close()
            gc.collect()

        # create rats
        # flip the restriction lookup so it is {int: string}
        restriction_lookup = {v: k for k, v in self.restriction_lookup.items()}
        for r in ["forest", "og", "mine"]:
            tif = os.path.join(self.config["out_path"], r + "_restriction.tif")
            create_rat(tif, restriction_lookup)
        # and the designation/process_order rat
        tif = os.path.join(self.config["out_path"], "designatedlands.tif")
        designation_lookup = {
            int(s["process_order"]): s["designation"] for s in self.sources
        }
        create_rat(tif, designation_lookup)

    def get_tiles(self, table):
        """Return a list of all tiles present in supplied table
        """
        sql = """SELECT DISTINCT map_tile
                 FROM {table}
                 ORDER BY map_tile
              """.format(table=table)
        return [r[0] for r in self.db.query(sql)]

    def intersect(self, table_a, table_b, out_table, tiles=None):
        """
        Intersect table_a with table_b, creating out_table
        Inputs must not have columns with equivalent names
        """
        # examine the inputs to determine what columns should be in the output
        columns_a = [
            Column(c.name, c.type) for c in self.db["public." + table_a].sqla_columns
        ]
        columns_b = [
            Column(c.name, c.type) for c in self.db["public." + table_b].sqla_columns
        ]
        column_names_a = set([c.name for c in columns_a if c.name != "geom"])
        column_names_b = set([c.name for c in columns_b if c.name != "geom"])
        # test for non-unique columns in input (other than geom)
        non_unique_columns = column_names_a.intersection(column_names_b)
        if non_unique_columns:
            LOG.info(
                "Column(s) found in both sources: %s" % ",".join(non_unique_columns)
            )
            raise RuntimeError("Input column names must be unique")

        # make sure tile is not present in input tables
        if "intersect_tile" in (list(column_names_a) + list(column_names_b)):
            raise RuntimeError(
                "Column with name 'intersect_tile' may not be present in inputs"
            )

        # create output table
        self.db.execute(f"DROP TABLE IF EXISTS {out_table}")

        # add primary key
        pk = Column(out_table.split(".")[1] + "_id", Integer, primary_key=True)

        # remove geom and tile from columns list
        a = [c for c in columns_a if c.name != "geom" and c.name != "tile"]
        b = [c for c in columns_b if c.name != "geom" and c.name != "tile"]
        pgdata.Table(
            self.db,
            "designatedlands",
            out_table.split(".")[1],
            [pk]
            + a
            + b
            + [Column("intersect_tile", UnicodeText), Column("geom", Geometry)],
        )

        # populate the output table
        query = "intersect"
        tile_table = "tiles"
        sql = self.db.build_query(
            self.db.queries[query],
            {
                "table_a": table_a,
                "columns_a": ", ".join(column_names_a),
                "table_b": table_b,
                "columns_b": ", ".join(column_names_b),
                "out_table": out_table,
                "tile_table": tile_table,
            },
        )

        if not tiles:
            tiles = self.get_tiles(table_b, "tiles")
        func = partial(parallel_tiled, self.db.url, sql)
        pool = multiprocessing.Pool(processes=self.config["n_processes"])
        # add a progress bar
        results_iter = pool.imap_unordered(func, tiles)
        with click.progressbar(results_iter, length=len(tiles)) as bar:
            for _ in bar:
                pass
        pool.close()
        pool.join()

        # delete any records with empty geometries in the out table
        self.db.execute(
            """DELETE FROM {t} WHERE ST_IsEmpty(geom) = True
                   """.format(
                t=out_table
            )
        )

        # add map_tile index to output
        self.db.execute(
            """CREATE INDEX ON {t} (intersect_tile text_pattern_ops)
                   """.format(
                t=out_table
            )
        )

    def cleanup(self):
        # drop the source and preprocess tables
        LOG.info("Dropping all src_ and _preprc tables")
        for source in self.sources:
            for t in (source["src"], source["preprc"]):
                self.db.execute(f"DROP TABLE IF EXISTS {t}")


@click.group()
def cli():
    pass


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@verbose_opt
@quiet_opt
def test_connection(config_file, verbose, quiet):
    """Confirm that connection to postgres is successful
    """
    set_log_level(verbose, quiet)
    DL = DesignatedLands(config_file)
    if DL.db:
        click.echo(
            "Connection to {db_url} successful".format(db_url=DL.config["db_url"])
        )


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@click.option(
    "--designation", "-d", help="The 'designation' key for the source of interest"
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite any existing output, force fresh download",
)
@verbose_opt
@quiet_opt
def download(config_file, designation, overwrite, verbose, quiet):
    """Download data, load to postgres
    """
    set_log_level(verbose, quiet)
    DL = DesignatedLands(config_file)
    DL.download(designation=designation, overwrite=overwrite)


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@click.option(
    "--designation", "-a", help="The 'designation' key for the source of interest"
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite any existing output, force fresh download",
)
@verbose_opt
@quiet_opt
def preprocess(config_file, designation, overwrite, verbose, quiet):
    """Create tiles layer and preprocess sources where required"""
    set_log_level(verbose, quiet)
    DL = DesignatedLands(config_file)
    DL.preprocess(designation=designation)
    DL.create_bc_boundary()


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@verbose_opt
@quiet_opt
def process_vector(config_file, verbose, quiet):
    """Create vector designation/restriction layers"""
    set_log_level(verbose, quiet)
    DL = DesignatedLands(config_file)
    try:
        DL.create_designations_overlapping()
        LOG.info("designations_overlapping created successfully")
    except Exception as e:
        LOG.error(f"Error creating designations_overlapping: {e}", exc_info=True)
        raise
    try:
        DL.create_designations_planarized()
        LOG.info("designations_planarized created successfully")
    except Exception as e:
        LOG.error(f"Error creating designations_planarized: {e}", exc_info=True)
        raise


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@verbose_opt
@quiet_opt
def process_raster(config_file, verbose, quiet):
    """Create raster designation/restriction layers"""
    set_log_level(verbose, quiet)
    DL = DesignatedLands(config_file)
    DL.rasterize()
    DL.overlay_rasters()


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@verbose_opt
@quiet_opt
def dump(config_file, verbose, quiet):
    """Dump output tables to file"""
    set_log_level(verbose, quiet)
    DL = DesignatedLands(config_file)
    # create output folder if it does not exist
    Path(DL.config["out_path"]).mkdir(parents=True, exist_ok=True)
    # delete existing output gpkg if it exists
    out_file = Path(DL.config["out_path"]) / "designatedlands.gpkg"
    if out_file.exists():
        out_file.unlink()
    
    # Use ogr2ogr directly with subprocess for fresh database connections
    # This ensures we see committed data from other connections
    
    try:
        # Try to dump designations_overlapping
        LOG.info("Attempting to dump designations_overlapping...")
        cmd = [
            "ogr2ogr",
            "-f", "GPKG",
            str(out_file),
            "-nln", "designations_overlapping",
            DL.db.ogr_string,
            "-sql", "SELECT designations_overlapping_id, designation, source_id, source_name, forest_restriction, mine_restriction, og_restriction, map_tile, geom FROM public.designations_overlapping"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            LOG.info("designations_overlapping dumped successfully")
        else:
            LOG.warning(f"designations_overlapping dump failed: {result.stderr}")
    except Exception as e:
        LOG.warning(f"Error dumping designations_overlapping: {e}")
    
    try:
        # Try to dump designations_planarized
        LOG.info("Attempting to dump designations_planarized...")
        cmd = [
            "ogr2ogr",
            "-f", "GPKG",
            "-append",
            str(out_file),
            "-nln", "designations_planarized",
            DL.db.ogr_string,
            "-sql", "SELECT designations_planarized_id, array_to_string(designation,';') as designations, array_to_string(source_id,';') as source_ids, array_to_string(source_name,';') as source_names, array_to_string(forest_restrictions,';') as forest_restrictions, array_to_string(mine_restrictions,';') as mine_restrictions, array_to_string(og_restrictions,';') as og_restrictions, forest_restriction_max, mine_restriction_max, og_restriction_max, map_tile, geom FROM public.designations_planarized"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            LOG.info("designations_planarized dumped successfully")
        else:
            LOG.warning(f"designations_planarized dump failed: {result.stderr}")
    except Exception as e:
        LOG.warning(f"Error dumping designations_planarized: {e}")


@cli.command()
@click.argument("in_file", type=click.Path(exists=True))
@click.argument("out_file")
@click.argument("config_file", type=click.Path(exists=True), required=False)
@click.option("--in_layer", "-l", help="Name of input layer")
@click.option("--out_layer", "-nln", help="Name of output layer")
@verbose_opt
@quiet_opt
def overlay(in_file, out_file, config_file, in_layer, out_layer, verbose, quiet):
    """Intersect layer with designatedlands and write to GPKG
    """
    set_log_level(verbose, quiet)
    DL = DesignatedLands(config_file)

    if not in_layer:
        in_layer = fiona.listlayers(in_file)[0]

    if not out_layer:
        out_layer = in_layer

    # maximum table name length is 63, trim in_layer just in case
    new_layer_name = in_layer[:63].lower()
    overlay_layer = new_layer_name[:50] + "_overlay"

    # drop the tables if they exist
    DL.db.execute(f"DROP TABLE IF EXISTS designatedlands.{new_layer_name}")
    DL.db.execute(f"DROP TABLE IF EXISTS designatedlands.{overlay_layer}")

    # load input layer to postgres to public schema (intersect expects tables in public schema)
    DL.db.ogr2pg(
        in_file, in_layer=in_layer, out_layer=new_layer_name, schema="public"
    )

    # pull distinct tiles iterable into a list
    # use direct SQL query to avoid stale table reference issues
    tiles = DL.db.query("SELECT DISTINCT map_tile FROM tiles ORDER BY map_tile").fetchall()
    tiles = [t[0] for t in tiles]

    # run the overlay - intersect designations_planarized with the input layer
    DL.intersect(
        "designations_planarized", new_layer_name, "designatedlands." + overlay_layer, tiles,
    )

    # dump overlay table to file
    DL.db.pg2ogr(
        f"SELECT * FROM designatedlands.{overlay_layer}",
        "GPKG",
        str(out_file),
        out_layer,
        geom_type="MULTIPOLYGON",
    )


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@verbose_opt
@quiet_opt
def cleanup(config_file, verbose, quiet):
    """Remove temporary tables
    """
    set_log_level(verbose, quiet)
    DL = DesignatedLands(config_file)
    DL.cleanup()


if __name__ == "__main__":
    cli()
