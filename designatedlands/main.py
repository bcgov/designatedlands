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

import click
from geoalchemy2 import Geometry
import rasterio
from rasterio import features
import pandas as pd
import geopandas as gpd
import numpy as np
from sqlalchemy.schema import Column
from sqlalchemy.types import Integer, UnicodeText
from affine import Affine

import pgdata
import designatedlands
from designatedlands.config import defaultconfig
from designatedlands import util

LOG = logging.getLogger(__name__)


class ConfigError(Exception):
    """Configuration key error"""


class ConfigValueError(Exception):
    """Configuration value error"""


class DesignatedLands(object):
    """ A class to hold the job's config, data and methods
    """

    def __init__(self, config_file=None):

        LOG.info("Initializing designatedlands v{}".format(designatedlands.__version__))

        # load default config
        self.config = defaultconfig.copy()

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

        # define valid restriction classes and assign raster values
        self.restriction_lookup = {
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

        # sort by hierarchy
        self.sources = sorted(designation_list, key=lambda k: int(k["hierarchy"]))

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

        # do some basic checks on the input csv to see if hierarchy and restriction classes make sense
        self.validate_sources()

        # create designation property, a list of dicts.
        # Initialize simply with {"hierarchy": n, "designation": val},
        self.designations = (
            pd.DataFrame(self.sources)
            .astype({"hierarchy": int})[["hierarchy", "designation"]]
            .drop_duplicates()
            .sort_values("hierarchy")
            .to_dict("records")
        )

        # add id column, convert hierarchy to filled string, strip other values
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

            source["hierarchy"] = str(source["hierarchy"]).zfill(2)
            source["src"] = (
                "designatedlands.src_"
                + str(source["id"]).zfill(2)
                + "_"
                + source["designation"]
            )
            source["preprc"] = source["src"] + "_preprc"
            source["dl"] = (
                "designatedlands.dl_"
                + source["hierarchy"]
                + "_"
                + source["designation"]
            )

        # read list of supporting layers and remove excluded rows
        supporting_list = [
            s for s in csv.DictReader(open(self.config["sources_supporting"]))
        ]

        # add id column
        for i, source in enumerate(supporting_list, start=(len(self.sources) + 1)):
            source["id"] = i
            source["hierarchy"] = "00"
            source["src"] = "designatedlands." + source["alias"]
        self.sources_supporting = supporting_list

        # load source csv to the db
        cmd = [
            "ogr2ogr",
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
        subprocess.run(cmd)

    def validate_sources(self):
        """Make sure hierarchy is sequential
        """
        # check that hierarchy numbers start at 1 and end at n designations
        h = list(set(int(d["hierarchy"]) for d in self.sources if d["exclude"] != "T"))
        if min(h) != 1:
            raise ValueError("Lowest hierarchy in source table must be 1")
        if min(h) + len(h) != max(h) + 1:
            raise ValueError(
                "Highest hierarchy value in source table must be equivalent "
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

    def download(self, alias=None, overwrite=False):
        """Download source data"""

        sources = self.sources_supporting + self.sources

        # if supplied a layer name, only process that layer
        if alias:
            sources = [s for s in sources if s["alias"] == alias]
            if not sources:
                raise ValueError("Alias %s does not exist" % alias)

        # download and load everything that we can automate
        for source in [s for s in sources if s["manual_download"] != "T"]:
            # drop table if exists
            if overwrite:
                self.db[source["src"]].drop()
            if source["src"] not in self.db.tables:
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
                        "designatedlands",
                        "--table",
                        # don't prefix table name with schema
                        source["src"].split(".")[1],
                    ]
                    if source["query"]:
                        cmd = cmd + ["--query", source["query"]]
                    LOG.info(" ".join(cmd))
                    subprocess.run(cmd)

                # run non-bcgw downloads
                else:
                    LOG.info("Loading " + source["src"])
                    file, layer = util.download_non_bcgw(
                        source["url"],
                        self.config["dl_path"],
                        source["file_in_url"],
                        source["layer_in_file"],
                        overwrite=overwrite,
                    )
                    self.db.ogr2pg(
                        file,
                        in_layer=layer,
                        # don't prefix table name with schema
                        out_layer=source["src"].split(".")[1],
                        sql=source["query"],
                        schema="designatedlands",
                    )
            else:
                LOG.info(source["src"] + " already loaded.")

        # find and load manually downloaded sources
        for source in [s for s in sources if s["manual_download"] == "T"]:
            file = os.path.join(self.config["dl_path"], source["file_in_url"])
            if not os.path.exists(file):
                raise Exception(file + " does not exist, download it manually")
            # drop table if exists
            if overwrite:
                self.db[source["src"]].drop()
            if source["src"] not in self.db.tables:
                self.db.ogr2pg(
                    file,
                    in_layer=source["layer_in_file"],
                    out_layer=source["src"],
                    sql=source["query"],
                    schema="designatedlands",
                )
            else:
                LOG.info(source["src"] + " already loaded.")

    def preprocess(self, alias=None):
        """
        Preprocess sources as specified
        Supported operations:
          - clip
          - union
        """
        preprocess_sources = [
            s for s in self.sources if s["preprocess_operation"] != ""
        ]
        if alias:
            preprocess_sources = [s for s in preprocess_sources if s["alias"] == alias]
        LOG.info("Preprocessing")
        for source in preprocess_sources:
            if source["preprocess_operation"] not in ["clip", "union"]:
                raise ValueError(
                    "Preprocess operation %s not supprted"
                    % source["preprocess_operation"]
                )
            self.db[source["preprc"]].drop()
            # call the specified preprocess function
            if source["preprocess_operation"] == "clip":
                if "designatedlands." + source["preprocess_args"] not in self.db.tables:
                    raise RuntimeError(
                        "Clip layer {l} not found. Ensure it is loaded".format(
                            l=source["preprocess_args"]
                        )
                    )
                LOG.info("Preprocessing " + source["src"])
                self.clip(
                    source["src"], source["preprocess_args"], source["preprc"],
                )
            elif source["preprocess_operation"] == "union":
                LOG.info("Preprocessing " + source["src"])
                self.union(
                    source["src"], source["preprocess_args"], source["preprc"],
                )

    def clip(self, in_table, clip_table, out_table):
        """Clip geometry of in_table by clip_table, writing output to out_table
        """
        columns = ["a." + c for c in self.db[in_table].columns if c != "geom"]
        sql = """CREATE TABLE {temp} AS
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
              """.format(
            temp=out_table,
            columns=", ".join(columns),
            in_table=in_table,
            clip_table=clip_table,
        )
        self.db.execute(sql)

    def union(self, in_table, columns, out_table):
        """Union/merge overlapping records with equivalent values for provided columns
        """
        sql = """CREATE TABLE {temp} AS
                 SELECT
                   {columns},
                   (ST_Dump(ST_Union(geom))).geom as geom
                 FROM {in_table}
                 GROUP BY {columns}
              """.format(
            temp=out_table, columns=columns, in_table=in_table
        )
        self.db.execute(sql)

    def tidy(self):
        """Create a single designatedlands table, holding all designations
        """

        # create output table
        out_table = "designatedlands.designatedlands"
        self.db[out_table].drop()
        LOG.info("Creating: {}".format(out_table))
        sql = """
        CREATE TABLE {out_table} (
          designatedlands_id serial PRIMARY KEY,
          hierarchy integer,
          designation text,
          source_id text,
          source_name text,
          forest_restriction text,
          og_restriction text,
          mine_restriction text,
          geom geometry
        );
        """.format(
            out_table=out_table
        )
        self.db.execute(sql)

        # insert data
        for source in self.sources:
            input_table = source["src"]
            if source["preprc"] in self.db.tables:
                input_table = source["preprc"]

            LOG.info(
                "Inserting data from {in_table} into {out_table}".format(
                    in_table=input_table, out_table=out_table
                )
            )
            lookup = {
                "out_table": out_table,
                "src_table": input_table,
                "hierarchy": source["hierarchy"],
                "desig_type": source["designation"],
                "source_id_col": source["source_id_col"],
                "source_name_col": source["source_name_col"],
                "forest_restriction": source["forest_restriction"],
                "og_restriction": source["og_restriction"],
                "mine_restriction": source["mine_restriction"],
            }
            sql = self.db.build_query(self.db.queries["merge"], lookup)
            self.db.execute(sql)

        # index geom
        self.db[out_table].create_index_geom()

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
        query = "SELECT * FROM designatedlands.bc_boundary_land"
        hierarchy = 0
        command = gdal_rasterize + [
            "-burn",
            f"{hierarchy}",
            "-sql",
            f"{query}",
            f"rasters/dl_{hierarchy}.tif",
        ]
        LOG.info(" ".join(command))
        subprocess.run(command)
        # then rasterize the rest
        for hierarchy in reversed(
            list(set([int(s["hierarchy"]) for s in self.sources]))
        ):
            query = f"SELECT * FROM designatedlands.designatedlands WHERE hierarchy={hierarchy}"
            command = gdal_rasterize + [
                "-burn",
                f"{hierarchy}",
                "-sql",
                f"{query}",
                f"rasters/dl_{hierarchy}.tif",
            ]
            LOG.info(" ".join(command))
            subprocess.run(command)

    def overlay_rasters(self):
        """Overlay raster designations to remove overlaps
        """
        LOG.info("Overlaying rasters")

        # initialize output rasters with BC boundary
        designation = rasterio.open("rasters/dl_0.tif").read(1)
        forest_restriction = designation.copy()
        og_restriction = designation.copy()
        mine_restriction = designation.copy()

        # loop backwards through designations
        for source in reversed(
            list(
                set(
                    [
                        (
                            int(s["hierarchy"]),
                            s["forest_restriction"],
                            s["og_restriction"],
                            s["mine_restriction"],
                        )
                        for s in self.sources
                    ]
                )
            )
        ):
            # unpack the values into individual variables
            (
                hierarchy_val,
                forest_restriction_val,
                og_restriction_val,
                mine_restriction_val,
            ) = source
            LOG.info("loading hierarchy n" + str(hierarchy_val))
            B = rasterio.open(f"rasters/dl_{hierarchy_val}.tif").read(1)
            # set output raster to hierarchy value
            LOG.info("calculating")

            # create index array pointing to cells we want to tag
            # (in BC, and with current hierarchy number)
            index_array = np.where(
                (designation >= 0) & (designation != 255) & (B == hierarchy_val),
                True,
                False,
            )

            # tag designations and restriction types
            designation[index_array] = hierarchy_val
            forest_restriction[index_array] = forest_restriction_val
            og_restriction[index_array] = og_restriction_val
            mine_restriction[index_array] = mine_restriction_val

        # define name of output tif for each array
        out_rasters = [
            (designation, "designatedlands"),
            (forest_restriction, "forest_restriction"),
            (og_restriction, "og_restriction"),
            (mine_restriction, "mine_restriction"),
        ]
        # write output rasters to disk
        Path(self.config["out_path"]).mkdir(parents=True, exist_ok=True)
        for out_raster in out_rasters:
            LOG.info("writing output raster %s" % out_raster[1])
            with rasterio.open(
                os.path.join(self.config["out_path"], out_raster[1] + ".tif"),
                "w",
                driver="GTiff",
                dtype="uint8",
                count=1,
                width=self.raster_profile["width"],
                height=self.raster_profile["height"],
                crs="EPSG:3005",
                transform=self.raster_profile["transform"],
                nodata=255,
            ) as dst:
                dst.write(out_raster[0], indexes=1)

    def get_tiles(self, table, tile_table="tiles_250k"):
        """Return a list of all tiles intersecting supplied table
        """
        sql = """SELECT DISTINCT b.map_tile
                 FROM {table} a
                 INNER JOIN {tile_table} b ON st_intersects(b.geom, a.geom)
                 ORDER BY map_tile
              """.format(
            table=table, tile_table=tile_table
        )
        return [r[0] for r in self.db.query(sql)]

    def intersect(self, table_a, table_b, out_table, tiles=None):
        """
        Intersect table_a with table_b, creating out_table
        Inputs must not have columns with equivalent names
        """
        # examine the inputs to determine what columns should be in the output
        columns_a = [Column(c.name, c.type) for c in self.db[table_a].sqla_columns]
        columns_b = [Column(c.name, c.type) for c in self.db[table_b].sqla_columns]
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
        self.db[out_table].drop()
        # add primary key
        pk = Column(out_table + "_id", Integer, primary_key=True)
        # remove geom and tile
        a = [c for c in columns_a if c.name != "geom" and c.name != "tile"]
        b = [c for c in columns_b if c.name != "geom" and c.name != "tile"]
        g = [c for c in columns_a if c.name == "geom"]
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
        func = partial(util.parallel_tiled, self.db.url, sql)
        pool = multiprocessing.Pool(processes=self.config["n_processes"])
        # add a progress bar
        results_iter = pool.imap_unordered(func, tiles)
        with click.progressbar(results_iter, length=len(tiles)) as bar:
            for _ in bar:
                pass
        # pool.map(func, tiles)
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
            self.db[source["src"]].drop()
            self.db[source["preprc"]].drop()
