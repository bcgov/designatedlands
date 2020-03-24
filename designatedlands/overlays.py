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

from sqlalchemy.schema import Column
from sqlalchemy.types import Integer

import click
import pgdata

from designatedlands import util

LOG = logging.getLogger(__name__)


def get_tiles(db, table, tile_table="a00_tiles_250k"):
    """Return a list of all intersecting tiles from specified layer
    """
    sql = """SELECT DISTINCT b.map_tile
             FROM {table} a
             INNER JOIN {tile_table} b ON st_intersects(b.geom, a.geom)
             ORDER BY map_tile
          """.format(
        table=table, tile_table=tile_table
    )
    return [r[0] for r in db.query(sql)]


def parse_tiles(db, tiles):
    """
    Translate a comma separated string holding tile names in either 20k or 250k
    format to a list of 20k tiles (allows passing values like '093')
    """
    tilelist = []
    if tiles:
        for tile_token in tiles.split(","):
            sql = "SELECT DISTINCT map_tile FROM tiles WHERE map_tile LIKE %s"
            tiles20 = [r[0] for r in db.query(sql, (tile_token + "%",)).fetchall()]
            tilelist = tilelist + tiles20
    return tilelist


def parallel_tiled(db_url, sql, tile, n_subs=2):
    """
    Create a connection and execute query for specified tile
    n_subs is the number of places in the sql query that should be
    substituted by the tile name
    """
    db = pgdata.connect(db_url, schema="public", multiprocessing=True)
    # As we are explicitly splitting up our job by tile and processing tiles
    # concurrently in individual connections we don't want the database to try
    # and manage parallel execution of these queries within these connections.
    # Turn off this connection's parallel execution:
    db.execute("SET max_parallel_workers_per_gather = 0")
    db.execute(sql, (tile + "%",) * n_subs)


def clip(db, in_table, clip_table, out_table):
    """Clip geometry of in_table by clip_table, writing output to out_table
    """
    columns = ["a." + c for c in db[in_table].columns if c != "geom"]
    db[out_table].drop()
    sql = """CREATE UNLOGGED TABLE {temp} AS
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
    LOG.info("Clipping %s by %s to create %s" % (in_table, clip_table, out_table))
    db.execute(sql)


def union(db, in_table, columns, out_table):
    """Union/merge overlapping records with equivalent values for provided columns
    """
    db[out_table].drop()
    sql = """CREATE UNLOGGED TABLE {temp} AS
             SELECT
               {columns},
               (ST_Dump(ST_Union(geom))).geom as geom
             FROM {in_table}
             GROUP BY {columns}
          """.format(
        temp=out_table, columns=columns, in_table=in_table
    )
    LOG.info(
        "Unioning geometries in %s by %s to create %s" % (in_table, columns, out_table)
    )
    db.execute(sql)


def tile_sources(db, source_csv, alias=None, force=False):
    """
    - merge/union data within sources
    - cut sources by tile
    - repair source geom
    - add required columns
    """
    sources = util.read_csv(source_csv)
    # process only the source layer specified
    if alias:
        sources = [s for s in sources if s["alias"] == alias]
    # for all designated lands sources:
    # - create new table name prefixed with b_<hierarchy>
    # - create and populate standard columns:
    #     - designation (equivalent to source's alias in sources.csv)
    #     - designation_id (unique id of source feature)
    #     - designation_name (name of source feature)
    tile_sources = [s for s in sources if s["exclude"] != "T" and s["hierarchy"] != 0]
    for source in tile_sources:
        if source["tiled_table"] not in db.tables or force:
            LOG.info("Tiling and validating: %s" % source["alias"])
            db[source["tiled_table"]].drop()
            lookup = {
                "out_table": source["tiled_table"],
                "src_table": source["input_table"],
                "designation_id_col": source["designation_id_col"],
                "designation_name_col": source["designation_name_col"],
            }
            sql = db.build_query(db.queries["prep1_merge_tile_a"], lookup)
            db.execute(sql)


def clean_and_agg_sources(db, source_csv, alias=None, force=False):
    """
    After sources are tiled and preprocessed, aggregation and cleaning is
    helpful to reduce topology exceptions in further processing. This is
    separate from the tiling / preprocessing because non-aggregated outputs
    (with the source designation name and id) are required.
    """
    sources = util.read_csv(source_csv)
    # process only the source layer specified
    if alias:
        sources = [s for s in sources if s["alias"] == alias]
    # for all designated lands sources:
    # - create new table name prefixed with c_<hierarchy>
    # - aggregate by designation, tile
    clean_sources = [s for s in sources if s["exclude"] != "T" and s["hierarchy"] != 0]
    for source in clean_sources:
        if source["cleaned_table"] not in db.tables or force:
            LOG.info("Cleaning and aggregating: %s" % source["alias"])
            db[source["cleaned_table"]].drop()
            lookup = {
                "out_table": source["cleaned_table"],
                "src_table": source["tiled_table"],
            }
            sql = db.build_query(db.queries["prep2_clean_agg"], lookup)
            db.execute(sql)


def preprocess(db, source_csv, alias=None, force=False):
    """
    Preprocess sources as specified in source_csv
    Supported operations:
      - clip
      - union
    """
    sources = util.read_csv(source_csv)
    # process only the source layer specified
    if alias:
        sources = [s for s in sources if s["alias"] == alias]
    preprocess_sources = [s for s in sources if s["preprocess_operation"] != ""]
    for source in preprocess_sources:
        if source["input_table"] + "_preprc" not in db.tables or force:
            LOG.info("Preprocessing: %s" % source["alias"])
            if source["preprocess_operation"] not in ["clip", "union"]:
                raise ValueError(
                    "Preprocess operation %s not supprted"
                    % source["preprocess_operation"]
                )

            # prefix clip layer name with 'a00', only non tiled, non hierarchy
            # clip layers are suppported
            if source["preprocess_operation"] == "clip":
                source["preprocess_args"] = "a00_" + source["preprocess_args"]
            # call the specified preprocess function
            globals()[source["preprocess_operation"]](
                db,
                source["input_table"],
                source["preprocess_args"],
                source["input_table"] + "_preprc",
            )
            # overwrite the tiled table with the preprocessed table, but
            # retain the _preprc table as a flag that the job is done
            db[source["input_table"]].drop()
            db.execute(
                """CREATE TABLE {t} AS
                   SELECT * FROM {temp}
                """.format(
                    t=source["input_table"], temp=source["input_table"] + "_preprc"
                )
            )
            # re-create spatial index
            db[source["input_table"]].create_index_geom()


def create_bc_boundary(db, n_processes):
    """
    Create a comprehensive land-marine layer by combining three sources.

    Note that specificly named source layers are hard coded and must exist:
    - bc_boundary_land (BC boundary layer from GeoBC, does not include marine)
    - bc_abms (BC Boundary, ABMS)
    - marine_ecosections (BC Marine Ecosections)
    """
    # create land/marine definition table
    db.execute(db.queries["create_bc_boundary"])
    # Prep boundary sources
    # First, combine ABMS boundary and marine ecosections
    db["bc_boundary_marine"].drop()
    db.execute(
        """CREATE TABLE a00_bc_boundary_marine AS
                  SELECT
                    'bc_boundary_marine' as designation,
                     ST_Union(geom) as geom FROM
                      (SELECT st_union(geom) as geom
                       FROM a00_bc_abms
                       UNION ALL
                       SELECT st_union(geom) as geom
                       FROM a00_marine_ecosections) as foo
                   GROUP BY designation"""
    )
    for source in ["a00_bc_boundary_land", "a00_bc_boundary_marine"]:
        LOG.info("Prepping and inserting into bc_boundary: %s" % source)
        # subdivide before attempting to tile
        db["temp_" + source].drop()
        db.execute(
            """CREATE UNLOGGED TABLE temp_{t} AS
                      SELECT ST_Subdivide(geom) as geom
                      FROM {t}""".format(
                t=source
            )
        )
        db["temp_" + source].create_index_geom()
        # tile
        db[source + "_tiled"].drop()
        lookup = {"src_table": "temp_" + source, "out_table": source + "_tiled"}
        db.execute(db.build_query(db.queries["prep1_merge_tile_b"], lookup))
        db["temp_" + source].drop()
        # combine the boundary layers into new table bc_boundary
        sql = db.build_query(
            db.queries["populate_output"],
            {"in_table": source + "_tiled", "out_table": "bc_boundary"},
        )
        tiles = get_tiles(db, source + "_tiled", "tiles")
        func = partial(parallel_tiled, db.url, sql)
        pool = multiprocessing.Pool(processes=n_processes)
        pool.map(func, tiles)
        pool.close()
        pool.join()
    # rename the 'designation' column
    db.execute(
        """ALTER TABLE bc_boundary
                  RENAME COLUMN designation TO bc_boundary"""
    )


def intersect(db, in_table, intersect_table, out_table, n_processes, tiles=None):
    """
    Intersect in_table with intersect_table, creating out_table
    Inputs may not have equivalently named columns
    """
    # examine the inputs to determine what columns should be in the output
    in_columns = [Column(c.name, c.type) for c in db[in_table].sqla_columns]
    intersect_columns = [
        Column(c.name, c.type)
        for c in db[intersect_table].sqla_columns
        if c.name not in ["geom", "map_tile"]
    ]
    # make sure output column names are unique, removing geom and map_tile from
    # the list as they are hard coded into the query
    in_names = set(
        [c.name for c in in_columns if c.name != "geom" and c.name != "map_tile"]
    )
    intersect_names = set([c.name for c in intersect_columns])
    # test for non-unique columns in input (other than map_tile and geom)
    non_unique_columns = in_names.intersection(intersect_names)
    if non_unique_columns:
        LOG.info("Column(s) found in both sources: %s" % ",".join(non_unique_columns))
        raise Exception("Input column names must be unique")

    # create output table
    db[out_table].drop()
    # add primary key
    pk = Column(out_table + "_id", Integer, primary_key=True)
    pgdata.Table(db, "public", out_table, [pk] + in_columns + intersect_columns)
    # populate the output table
    if "map_tile" not in [c.name for c in db[intersect_table].sqla_columns]:
        query = "intersect_inputtiled"
        tile_table = "tiles"
        sql = db.build_query(
            db.queries[query],
            {
                "in_table": in_table,
                "in_columns": ", ".join(in_names),
                "intersect_table": intersect_table,
                "intersect_columns": ", ".join(intersect_names),
                "out_table": out_table,
                "tile_table": tile_table,
            },
        )
    else:
        query = "intersect_alltiled"
        tile_table = None
        sql = db.build_query(
            db.queries[query],
            {
                "in_table": in_table,
                "in_columns": ", ".join(in_names),
                "intersect_table": intersect_table,
                "intersect_columns": ", ".join(intersect_names),
                "out_table": out_table,
            },
        )
    if not tiles:
        tiles = get_tiles(db, intersect_table, "tiles")
    func = partial(parallel_tiled, db.url, sql)
    pool = multiprocessing.Pool(processes=n_processes)
    # add a progress bar
    results_iter = pool.imap_unordered(func, tiles)
    with click.progressbar(results_iter, length=len(tiles)) as bar:
        for _ in bar:
            pass
    # pool.map(func, tiles)
    pool.close()
    pool.join()
    # delete any records with empty geometries in the out table
    db.execute(
        """DELETE FROM {t} WHERE ST_IsEmpty(geom) = True
               """.format(
            t=out_table
        )
    )
    # add map_tile index to output
    db.execute(
        """CREATE INDEX {t}_tileix
                  ON {t} (map_tile text_pattern_ops)
               """.format(
            t=out_table
        )
    )
