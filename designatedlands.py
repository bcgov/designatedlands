# Copyright 2017 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

import os
import logging
import tempfile
from urlparse import urlparse
import shutil
import urllib2
import zipfile
import tarfile
import csv
import subprocess
import hashlib
import multiprocessing
from functools import partial
from xml.sax.saxutils import escape

from sqlalchemy.schema import Column
from sqlalchemy.types import Integer
import requests
import click
import fiona

import bcdata
import pgdb

import psycopg2


# --------------------------------------------
# Change default database/paths/filenames etc here (see README.md)
# --------------------------------------------
CONFIG = {
    "source_data": "source_data",
    "source_csv": "sources_test.csv",
    "out_table": "designatedlands",
    "out_table_overlaps": "designatedlands_overlaps",
    "out_file": "designatedlands.gpkg",
    "out_format": "GPKG",
    "db_url":
    "postgresql://postgres:postgres@localhost:5432/designatedlands",
    "n_processes": multiprocessing.cpu_count() - 1
    }
# --------------------------------------------
# --------------------------------------------

CHUNK_SIZE = 1024

logging.basicConfig(level=logging.INFO)

HELP = {
  "csv": 'Path to csv that lists all input data sources',
  "email": 'A valid email address, used for DataBC downloads',
  "dl_path": 'Path to folder holding downloaded data',
  "alias": "The 'alias' key identifing the source of interest, from source csv",
  "out_file": "Output geopackage name",
  "out_format": "Output format. Default GPKG (Geopackage)",
  "out_table": 'Output designated lands postgres table'}


def get_files(path):
    """Returns an iterable containing the full path of all files in the
    specified path.
    https://github.com/OpenBounds/Processing/blob/master/utils.py
    """
    if os.path.isdir(path):
        for (dirpath, dirnames, filenames) in os.walk(path):
            for filename in filenames:
                if not filename[0] == '.':
                    yield os.path.join(dirpath, filename)
    else:
        yield path


def read_csv(path):
    """
    Returns list of dicts from file, sorted by 'hierarchy' column
    https://stackoverflow.com/questions/72899/
    """
    source_list = [source for source in csv.DictReader(open(path, 'rb'))]
    # convert hierarchy value to integer
    for source in source_list:
        source.update((k, int(v)) for k, v in source.iteritems()
                      if k == "hierarchy" and v != '')
        # for convenience, add the layer names to the dict
        hierarchy = str(source["hierarchy"]).zfill(2)
        clean_table = "c"+hierarchy+"_"+source["alias"]
        source.update({"src_table": source["alias"],
                       "clean_table": clean_table})
    return sorted(source_list, key=lambda k: k['hierarchy'])


def make_sure_path_exists(path):
    """
    Make directories in path if they do not exist.
    Modified from http://stackoverflow.com/a/5032238/1377021
    """
    try:
        os.makedirs(path)
        return path
    except:
        pass


def get_path_parts(path):
    """Splits a path into parent directories and file.
    """
    return path.split(os.sep)


def download_bcgw(url, dl_path, email=None, gdb=None):
    """Download BCGW data using DWDS
    """
    # make sure an email is provided
    if not email:
        email = os.environ["BCDATA_EMAIL"]
    if not email:
        raise Exception("An email address is required to download BCGW data")
    # check that the extracted download isn't already in tmp
    if gdb and os.path.exists(os.path.join(dl_path, gdb)):
        return os.path.join(dl_path, gdb)
    else:
        download = bcdata.download(url, email)
        if not download:
            raise Exception("Failed to create DWDS order")
        # move the downloaded .gdb to specified dl_path
        out_gdb = os.path.split(download)[1]
        shutil.copytree(download, os.path.join(dl_path, out_gdb))
        return os.path.join(dl_path, out_gdb)


def download_non_bcgw(url, download_cache=None):
    """Download a file to location specified
    Modified from https://github.com/OpenBounds/Processing/blob/master/utils.py
    """
    info('Downloading', url)

    parsed_url = urlparse(url)

    urlfile = parsed_url.path.split('/')[-1]
    _, extension = os.path.split(urlfile)

    fp = tempfile.NamedTemporaryFile('wb', dir=download_cache,
                                     suffix=extension, delete=False)
    if not download_cache:
        download_cache = tempfile.gettempdir()

    cache_path = os.path.join(download_cache,
                              hashlib.sha224(url).hexdigest())
    if os.path.exists(cache_path):
        info("Returning %s from local cache" % url)
        fp.close()
        shutil.copy(cache_path, fp.name)
        return fp

    if parsed_url.scheme == "http" or parsed_url.scheme == "https":
        res = requests.get(url, stream=True, verify=False)

        if not res.ok:
            raise IOError

        for chunk in res.iter_content(CHUNK_SIZE):
            fp.write(chunk)
    elif parsed_url.scheme == "ftp":
        download = urllib2.urlopen(url)

        file_size_dl = 0
        block_sz = 8192
        while True:
            buffer = download.read(block_sz)
            if not buffer:
                break

            file_size_dl += len(buffer)
            fp.write(buffer)

    fp.close()

    if cache_path:
        if not os.path.exists(download_cache):
            os.makedirs(download_cache)
        shutil.copy(fp.name, cache_path)

    return fp


def extract(fp, dl_path, alias, source_filename):
    """
    Unzip the archive, return path to specified file
    (this presumes that we already know the name of the desired file)
    Modified from https://github.com/OpenBounds/Processing/blob/master/utils.py
    """
    info('Extracting', fp.name)
    unzip_dir = make_sure_path_exists(os.path.join(dl_path, alias))
    info(unzip_dir)
    zipped_file = get_compressed_file_wrapper(fp.name)
    zipped_file.extractall(unzip_dir)
    zipped_file.close()
    return os.path.join(unzip_dir, source_filename)


def info(*strings):
    logging.info(' '.join(strings))


def error(*strings):
    logging.error(' '.join(strings))


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
        return zipfile.ZipFile(path, 'r')
    elif archive_format == ARCHIVE_FORMAT_TAR_GZ:
        return ZipCompatibleTarFile.open(path, 'r:gz')
    elif archive_format == ARCHIVE_FORMAT_TAR_BZ2:
        return ZipCompatibleTarFile.open(path, 'r:bz2')


def ogr2pg(db, in_file, in_layer=None, out_layer=None,
           schema='public', t_srs='EPSG:3005', sql=None):
    """
    Load a layer to provided pgdb database connection using OGR2OGR

    SQL provided is like the ESRI where_clause, but in SQLITE dialect:
    SELECT * FROM <in_layer> WHERE <sql>
    """
    # if not provided a layer name, use the name of the input file
    if not in_layer:
        in_layer = os.path.splitext(os.path.basename(in_file))[0]
    if not out_layer:
        out_layer = in_layer.lower()
    command = ['ogr2ogr',
               '--config PG_USE_COPY YES',
               '-t_srs '+t_srs,
               '-f PostgreSQL',
               '''PG:"host={h} user={u} dbname={db} password={pwd}"'''.format(
                          h=db.host,
                          u=db.user,
                          db=db.database,
                          pwd=db.password),
               '-lco OVERWRITE=YES',
               '-overwrite',
               '-lco SCHEMA={schema}'.format(schema=schema),
               '-lco GEOMETRY_NAME=geom',
               '-dim 2',
               '-nln '+out_layer,
               '-nlt PROMOTE_TO_MULTI',
               in_file,
               in_layer]

    if sql:
        command.insert(4,
                       '-sql "SELECT * FROM %s WHERE %s" -dialect SQLITE' %
                       (in_layer, sql))
        # remove layer name, it is ignored in combination with sql
        command.pop()
    info('Loading %s to %s' % (out_layer, db.url))
    subprocess.call(" ".join(command), shell=True)


def get_tiles(db, table, tile_table="tiles_250k"):
    """Return a list of all intersecting tiles from specified layer
    """
    sql = """SELECT DISTINCT b.map_tile
             FROM {table} a
             INNER JOIN {tile_table} b ON st_intersects(b.geom, a.geom)
             ORDER BY map_tile
          """.format(table=table,
                     tile_table=tile_table)
    return [r[0] for r in db.query(sql)]


def parallel_tiled(sql, tile):
    """Create a connection and execute query for specified tile
    """
    db = pgdb.connect(CONFIG["db_url"], schema="public")
    db.execute(sql, (tile+"%", tile+"%"))


def clip(db, in_table, clip_table):
    """
    Clip geometry of in_table by clip_table, overwriting in_table with the
    output.
    """
    columns = ["a."+c for c in db[in_table].columns if c != 'geom']
    temp_table = "temp_clip"
    db[temp_table].drop()
    sql = """CREATE UNLOGGED TABLE {temp} AS
             SELECT
               {columns},
               CASE
                 WHEN ST_CoveredBy(a.geom, b.geom) THEN a.geom
                 ELSE ST_Multi(
                        ST_Intersection(a.geom,b.geom)) END AS geom
             FROM {in_table} AS a
             INNER JOIN {clip_table} AS b
             ON ST_Intersects(a.geom, b.geom)
          """.format(temp=temp_table,
                     columns=", ".join(columns),
                     in_table=in_table,
                     clip_table=clip_table)
    info('Clipping %s by %s' % (in_table, clip_table))
    db.execute(sql)
    # drop the source table
    db[in_table].drop()
    # copy the temp output back to the source
    db.execute("""CREATE TABLE {t} AS
                  SELECT * FROM {temp}""".format(t=in_table, temp=temp_table))
    # re-create indexes
    db[in_table].create_index_geom()
    db[in_table].create_index(["id"])


def preprocess(db, source_csv, alias=None, rem_overlaps=True):
    """
    Before running the main processing job:
      - create comprehensive tiling layer
      - preprocess sources as specified in source_csv
    """
    sources = read_csv(source_csv)

    if rem_overlaps:
      clean_query = "clean_union"
    else:
      clean_query = "clean_union_overlaps"

    if alias:
        sources = [s for s in sources if s['alias'] == alias]

    # create tile layer
    db.execute(db.queries["create_tiles"])

    # for all designated lands sources:
    # - union/merge polygons
    # - create new table name prefixed with hierarchy
    # - retain just one column (designation), value equivalent to table name
    clean_sources = [s for s in sources
                     if s["exclude"] != 'T' and s['hierarchy'] != 0]
    for source in clean_sources:
        info("Tiling - dissolving - validating: %s" % source["alias"])
        db[source["clean_table"]].drop()
        lookup = {"out_table": source["clean_table"],
                  "src_table": source["src_table"]}
        if not rem_overlaps:
          lookup.update({"designation_id_col": source["designation_id_col"], 
            "designation_name_col": source["designation_name_col"]})
        sql = db.build_query(db.queries[clean_query], lookup)
        db.execute(sql)

    # apply pre-processing operation specified in sources.csv
    preprocess_sources = [s for s in sources
                          if s["preprocess_operation"] != '']
    for source in preprocess_sources:
        # what is the cleaned name of the pre-process layer?
        preprocess_lyr = [s for s in sources
                          if s["alias"] == source["preprocess_layer_alias"]][0]
        function = source["preprocess_operation"]
        # call the specified preprocess function
        globals()[function](db,
                            source["clean_table"],
                            preprocess_lyr["alias"])


def create_bc_boundary(db, n_processes):
    """
    Create a comprehensive land-marine layer by combining three sources.

    Note that specificly named source layers are hard coded and must exist:
    - bc_boundary_land (BC boundary layer from GeoBC, does not include marine)
    - bc_abms (BC Boundary, ABMS)
    - marine_ecosections (BC Marine Ecosections)
    """
    # create land/marine definition table
    target_table = "bc_boundary"
    db[target_table].drop()
    db.execute(db.build_query(db.queries['create_target'],
                              {"table": target_table}))
    # Prep boundary sources and insert into out layer
    # First, combine ABMS boundary and marine ecosections
    db["bc_boundary_marine"].drop()
    db.execute("""CREATE TABLE bc_boundary_marine AS
                  SELECT
                    'bc_boundary_marine' as designation,
                     ST_Union(geom) as geom FROM
                      (SELECT st_union(geom) as geom
                       FROM bc_abms
                       UNION ALL
                       SELECT st_union(geom) as geom
                       FROM marine_ecosections) as foo
                   GROUP BY designation""")

    for source in ["bc_boundary_land", "bc_boundary_marine"]:
        info('Prepping and inserting into bc_boundary: %s' % source)
        # subdivide before attempting to tile
        db["temp_"+source].drop()
        db.execute("""CREATE UNLOGGED TABLE temp_{t} AS
                      SELECT ST_Subdivide(geom) as geom
                      FROM {t}""".format(t=source))
        db["temp_"+source].create_index_geom()
        # tile
        db[source+"_tiled"].drop()
        lookup = {"src_table": "temp_"+source,
                  "out_table": source+"_tiled"}
        db.execute(db.build_query(db.queries["clean_union"], lookup))
        db["temp_"+source].drop()

        # combine the boundary layers into new table bc_boundary
        sql = db.build_query(db.queries["populate_target"],
                             {"in_table": source+"_tiled",
                              "out_table": "bc_boundary"})
        tiles = get_tiles(db, source+"_tiled")
        func = partial(parallel_tiled, sql)
        pool = multiprocessing.Pool(processes=3)
        pool.map(func, tiles)
        pool.close()
        pool.join()

    # rename the 'designation' column
    db.execute("""ALTER TABLE bc_boundary
                  RENAME COLUMN designation TO bc_boundary""")


def intersect(db, in_table, intersect_table, out_table, n_processes,
              tiles=None):
    """
    Intersect in_table with intersect_table, creating out_table
    Inputs may not have equivalently named columns
    """
    # examine the inputs to determine what columns should be in the output
    in_columns = [Column(c.name, c.type) for c in db[in_table].sqla_columns]
    intersect_columns = [Column(c.name, c.type)
                         for c in db[intersect_table].sqla_columns
                         if c.name not in ['geom', 'map_tile']]
    # make sure output column names are unique, removing geom and map_tile from
    # the list as they are hard coded into the query
    in_names = set([c.name for c in in_columns
                    if c.name != 'geom' and c.name != 'map_tile'])
    intersect_names = set([c.name for c in intersect_columns])

    # test for non-unique columns in input (other than map_tile and geom)
    non_unique_columns = in_names.intersection(intersect_names)
    if non_unique_columns:
        info('Column(s) found in both sources: %s' %
             ",".join(non_unique_columns))
        raise Exception("Input column names must be unique")
    # create output table
    db[out_table].drop()
    # add primary key
    pk = Column(out_table+"_id", Integer, primary_key=True)
    pgdb.Table(db, "public", out_table, [pk]+in_columns+intersect_columns)
    # populate the output table
    if 'map_tile' not in [c.name for c in db[intersect_table].sqla_columns]:
        query = "intersect_inputtiled"
        tile_table = "tiles"
        sql = db.build_query(db.queries[query],
                             {"in_table": in_table,
                              "in_columns": ", ".join(in_names),
                              "intersect_table": intersect_table,
                              "intersect_columns": ", ".join(intersect_names),
                              "out_table": out_table,
                              "tile_table": tile_table})
    else:
        query = "intersect_alltiled"
        tile_table = None
        sql = db.build_query(db.queries[query],
                             {"in_table": in_table,
                              "in_columns": ", ".join(in_names),
                              "intersect_table": intersect_table,
                              "intersect_columns": ", ".join(intersect_names),
                              "out_table": out_table})
    if not tiles:
        tiles = get_tiles(db, intersect_table, "tiles")
    func = partial(parallel_tiled, sql)
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
    db.execute("""DELETE FROM {t} WHERE ST_IsEmpty(geom) = True
               """.format(t=out_table))
    # add map_tile index to output
    db.execute("""CREATE INDEX {t}_tileix
                  ON {t} (map_tile text_pattern_ops)
               """.format(t=out_table))


def postprocess(db, sources, out_table, n_processes, tiles=None):
    """Postprocess the output designated lands table
    """
    # add category (rollup) column by creating lookup table from source.csv
    lookup_data = [dict(alias=s["clean_table"],
                        category=s["category"])
                   for s in sources if s["category"]]
    # create lookup table
    db["category_lookup"].drop()
    db.execute("""CREATE TABLE category_lookup
                  (id SERIAL PRIMARY KEY, alias TEXT, category TEXT)""")
    db["category_lookup"].insert(lookup_data)

    # add category column
    if "category" not in db[out_table+"_prelim"].columns:
        db.execute("""ALTER TABLE {t}
                      ADD COLUMN category TEXT
                   """.format(t=out_table+"_prelim"))

    # populate category column from lookup
    db.execute("""UPDATE {t} AS o
                  SET category = lut.category
                  FROM category_lookup AS lut
                  WHERE o.designation = lut.alias
               """.format(t=out_table+"_prelim"))

    # Remove national park names from the national park tags
    sql = """UPDATE {t}
             SET designation = 'c01_park_national'
             WHERE designation LIKE 'c01_park_national%'
          """.format(t=out_table+"_prelim")
    db.execute(sql)
    # create marine-terrestrial layer
    if 'bc_boundary' not in db.tables:
        create_bc_boundary(db, n_processes)
    info('Cutting output layer with marine-terrestrial definition')
    intersect(db, out_table+"_prelim", "bc_boundary", out_table, n_processes,
              tiles)


def pg2ogr(db_url, sql, driver, outfile, outlayer=None, column_remap=None,
           geom_type=None):
    """
    A wrapper around ogr2ogr, for quickly dumping a postgis query to file.
    Suppported formats are ["ESRI Shapefile", "GeoJSON", "FileGDB", "GPKG"]
       - for GeoJSON, transforms to EPSG:4326
       - for Shapefile, consider supplying a column_remap dict
       - for FileGDB, geom_type is required
         (https://trac.osgeo.org/gdal/ticket/4186)
    """
    filename, ext = os.path.splitext(outfile)
    if not outlayer:
        outlayer = filename
    u = urlparse(db_url)
    pgcred = 'host={h} user={u} dbname={db} password={p}'.format(h=u.hostname,
                                                                 u=u.username,
                                                                 db=u.path[1:],
                                                                 p=u.password)
    # use a VRT so we can remap columns if a lookoup is provided
    if column_remap:
        # if specifiying output field names, all fields have to be specified
        # rather than try and parse the input sql, just do a test run of the
        # query and grab column names from that
        db = pgdb.connect(db_url)
        columns = [c for c in db.engine.execute(sql).keys() if c != 'geom']
        # make sure all columns are represented in the remap
        for c in columns:
            if c not in column_remap.keys():
                column_remap[c] = c
        field_remap_xml = " \n".join([
            '<Field name="'+column_remap[c]+'" src="'+c+'"/>'
            for c in columns])
    else:
        field_remap_xml = ""
    vrt = """<OGRVRTDataSource>
               <OGRVRTLayer name="{layer}">
                 <SrcDataSource>PG:{pgcred}</SrcDataSource>
                 <SrcSQL>{sql}</SrcSQL>
               {fieldremap}
               </OGRVRTLayer>
             </OGRVRTDataSource>
          """.format(layer=outlayer,
                     sql=escape(sql.replace("\n", " ")),
                     pgcred=pgcred,
                     fieldremap=field_remap_xml)
    vrtpath = os.path.join(tempfile.gettempdir(), filename+".vrt")
    if os.path.exists(vrtpath):
        os.remove(vrtpath)
    with open(vrtpath, "w") as vrtfile:
        vrtfile.write(vrt)
    # allow appending to filegdb and specify the geometry type
    if driver == 'FileGDB':
        nlt = "-nlt "+geom_type
        append = "-append"
    else:
        nlt = ""
        append = ""
    command = """ogr2ogr \
                    -progress \
                    -f "{driver}" {nlt} {append}\
                    {outfile} \
                    {vrt}
              """.format(driver=driver,
                         nlt=nlt,
                         append=append,
                         outfile=outfile,
                         vrt=vrtpath)
    # translate GeoJSON to EPSG:4326
    if driver == 'GeoJSON':
        command = command.replace("""-f "GeoJSON" """,
                                  """-f "GeoJSON" -t_srs EPSG:4326""")
    info(command)
    subprocess.call(command, shell=True)


@click.group()
def cli():
    pass


@cli.command()
@click.option('--drop', '-d', is_flag=True, help="Drop the existing database")
def create_db(drop):
    """Create an empty postgres db for processing
    """
    parsed_url = urlparse(CONFIG["db_url"])
    db_name = parsed_url.path
    db_name = db_name.strip('/')
    db = pgdb.connect("postgresql://"+parsed_url.netloc)
    if drop:
        db.execute("DROP DATABASE "+db_name)
    db.execute("CREATE DATABASE "+db_name)
    db = pgdb.connect(CONFIG["db_url"])
    db.execute("CREATE EXTENSION postgis")


@cli.command()
@click.option('--source_csv', '-s', default=CONFIG["source_csv"],
              type=click.Path(exists=True), help=HELP['csv'])
@click.option('--email', help=HELP['email'])
@click.option('--dl_path', default=CONFIG["source_data"],
              type=click.Path(exists=True), help=HELP['dl_path'])
@click.option('--alias', '-a', help=HELP['alias'])
def load(source_csv, email, dl_path, alias):
    """Download data, load to postgres
    """
    db = pgdb.connect(CONFIG["db_url"])
    sources = read_csv(source_csv)

    # filter sources based on optional provided alias and ignoring excluded
    if alias:
        sources = [s for s in sources if s["alias"] == alias]
    sources = [s for s in sources if s["exclude"] != 'T']

    # process sources where automated downloads are avaiable
    for source in [s for s in sources if s["manual_download"] != 'T']:
        info("Downloading %s" % source["alias"])

        # handle BCGW downloads
        if urlparse(source["url"]).hostname == 'catalogue.data.gov.bc.ca':
            gdb = source["layer_in_file"].split(".")[1] + ".gdb"
            file = download_bcgw(source["url"], dl_path, email=email, gdb=gdb)

        # handle all other downloads
        else:
            if os.path.exists(os.path.join(dl_path, source["alias"])):
                file = os.path.join(dl_path, source["alias"],
                                    source['file_in_url'])
            else:
                fp = download_non_bcgw(source['url'])
                file = extract(fp,
                               dl_path,
                               source['alias'],
                               source['file_in_url'])

        layer = get_layer_name(file, source["layer_in_file"])

        # load downloaded data to postgres
        ogr2pg(db,
               file,
               in_layer=layer,
               out_layer=source["alias"],
               sql=source["query"])

    # process manually downloaded sources
    for source in [s for s in sources if s["manual_download"] == 'T']:
        file = os.path.join(dl_path, source["file_in_url"])
        if not os.path.exists(file):
            raise Exception(file + " does not exist, download it manually")
        layer = get_layer_name(file, source["layer_in_file"])
        ogr2pg(db,
               file,
               in_layer=layer,
               out_layer=source["alias"],
               sql=source["query"])


# Check number of layers and only use layer name from sources.csv if > 1 layer, else use first
def get_layer_name(file, layer_name):
    layers = fiona.listlayers(file)
    if len(layers) > 1:
        layer = layer_name
    else:
        layer = layers[0]
    return layer


@cli.command()
@click.option('--source_csv', '-s', default=CONFIG["source_csv"],
              type=click.Path(exists=True), help=HELP['csv'])
@click.option('--out_table', '-o', default=CONFIG["out_table"],
              help=HELP["out_table"])
@click.option('--resume', '-r',
              help='hierarchy number at which to resume processing')
@click.option('--no_preprocess', is_flag=True,
              help="Do not preprocess input data")
@click.option('--n_processes', '-p', default=CONFIG["n_processes"],
              help="Number of parallel processing threads to utilize")
@click.option('--tiles', '-t', help="Comma separated list of tiles to process")
def process(source_csv, out_table, resume, no_preprocess, n_processes, tiles):
    """Create output designated lands table
    """
    if tiles:
        all_tiles = set(tiles.split(","))
    else:
        all_tiles = None
    db = pgdb.connect(CONFIG["db_url"], schema="public")
    db.execute(db.queries["safe_diff"])
    # run any required pre-processing
    # (no_preprocess flag is for development)
    if not no_preprocess:
        preprocess(db, source_csv)

    # create target table if not resuming from a bailed process
    if not resume:
        # create output table
        db[out_table+"_prelim"].drop()
        db.execute(db.build_query(db.queries['create_target'],
                                  {"table": out_table+"_prelim"}))

    # filter sources - use only non-exlcuded sources with hierarchy > 0
    sources = [s for s in read_csv(source_csv)
               if s['hierarchy'] != 0 and s["exclude"] != 'T']

    # in case of bailing during tests/development, specify resume option to
    # resume processing at specified hierarchy number
    if resume:
        p_sources = [s for s in sources if int(s["hierarchy"]) >= int(resume)]
    else:
        p_sources = sources

    # Using the tiles layer, fill in gaps so all BC is included in output
    # add 'id' to tiles table to match schema of other sources
    if 'id' not in db['tiles'].columns:
        db.execute("ALTER TABLE tiles ADD COLUMN id integer")
        db.execute("UPDATE tiles SET id = tile_id")
    if 'designation' not in db['tiles'].columns:
        db.execute("ALTER TABLE tiles ADD COLUMN designation text")
    undesignated = {"clean_table": "tiles",
                    "category": None}
    p_sources.append(undesignated)

    # iterate through all sources
    for source in p_sources:
        info("Inserting %s into %s" % (source["clean_table"],
                                       out_table+"_prelim"))
        sql = db.build_query(db.queries["populate_target"],
                             {"in_table": source["clean_table"],
                              "out_table": out_table+"_prelim"})
        # Find distinct tiles in source (20k)
        src_tiles = set(db[source["clean_table"]].distinct('map_tile'))
        # only use tiles specified
        if all_tiles:
            tiles = all_tiles & src_tiles
        else:
            tiles = src_tiles
        # Process by 250k tiles by using this query instead
        # tiles = get_tiles(db, source["clean_table"])

        # for testing, run only one process and report on tile
        if n_processes == 1:
            for tile in tiles:
                info(tile)
                db.execute(sql, (tile + "%", tile + "%"))
        else:
            func = partial(parallel_tiled, sql)
            pool = multiprocessing.Pool(processes=n_processes)
            pool.map(func, tiles)
            pool.close()
            pool.join()

    # clean up the output
    postprocess(db, sources, out_table, n_processes, tiles)

@cli.command()
@click.option('--source_csv', '-s', default=CONFIG["source_csv"],
              type=click.Path(exists=True), help=HELP['csv'])
@click.option('--out_table', '-o', default=CONFIG["out_table_overlaps"],
              help=HELP["out_table"])
@click.option('--resume', '-r',
              help='hierarchy number at which to resume processing')
@click.option('--no_preprocess', is_flag=True,
              help="Do not preprocess input data")
@click.option('--n_processes', '-p', default=CONFIG["n_processes"],
              help="Number of parallel processing threads to utilize")
@click.option('--tiles', '-t', help="Comma separated list of tiles to process")
def process_overlaps(source_csv, out_table, resume, no_preprocess, n_processes, tiles):
    """Create output designated lands table
    """
    if tiles:
        all_tiles = set(tiles.split(","))
    else:
        all_tiles = None
    db = pgdb.connect(CONFIG["db_url"], schema="public")
    db.execute(db.queries["safe_diff"])
    # run any required pre-processing
    # (no_preprocess flag is for development)
    if not no_preprocess:
        preprocess(db, source_csv)

    # create target table if not resuming from a bailed process
    if not resume:
        # create output table
        db[out_table+"_prelim"].drop()
        db.execute(db.build_query(db.queries['create_target_overlaps'],
                                  {"table": out_table+"_prelim"}))

    # filter sources - use only non-exlcuded sources with hierarchy > 0
    sources = [s for s in read_csv(source_csv)
               if s['hierarchy'] != 0 and s["exclude"] != 'T']

    # in case of bailing during tests/development, specify resume option to
    # resume processing at specified hierarchy number
    if resume:
        p_sources = [s for s in sources if int(s["hierarchy"]) >= int(resume)]
    else:
        p_sources = sources

    # Using the tiles layer, fill in gaps so all BC is included in output
    # add 'id' to tiles table to match schema of other sources
    if 'id' not in db['tiles'].columns:
        db.execute("ALTER TABLE tiles ADD COLUMN id integer")
        db.execute("UPDATE tiles SET id = tile_id")
    if 'designation' not in db['tiles'].columns:
        db.execute("ALTER TABLE tiles ADD COLUMN designation text")
    if 'designation_id' not in db['tiles'].columns:
        db.execute("ALTER TABLE tiles ADD COLUMN designation_id text")
    if 'designation_name' not in db['tiles'].columns:
        db.execute("ALTER TABLE tiles ADD COLUMN designation_name text")
    undesignated = {"clean_table_overlaps": "tiles",
                    "category": None}
    p_sources.append(undesignated)

    # iterate through all sources
    for source in p_sources:
        info("Inserting %s into %s" % (source["clean_table_overlaps"],
                                       out_table+"_prelim"))
        sql = db.build_query(db.queries["populate_target_overlaps"],
                             {"in_table": source["clean_table_overlaps"],
                              "out_table": out_table+"_prelim"})
        # Find distinct tiles in source (20k)
        src_tiles = set(db[source["clean_table_overlaps"]].distinct('map_tile'))
        # only use tiles specified
        if all_tiles:
            tiles = all_tiles & src_tiles
        else:
            tiles = src_tiles
        # Process by 250k tiles by using this query instead
        # tiles = get_tiles(db, source["clean_table"])

        # for testing, run only one process and report on tile
        if n_processes == 1:
            for tile in tiles:
                info(tile)
                db.execute(sql, (tile + "%", tile + "%"))
        else:
            func = partial(parallel_tiled, sql)
            pool = multiprocessing.Pool(processes=n_processes)
            pool.map(func, tiles)
            pool.close()
            pool.join()

    # clean up the output
    postprocess(db, sources, out_table, n_processes, tiles)


@cli.command()
@click.argument('in_file', type=click.Path(exists=True))
@click.option('--in_layer', '-l', help="Input layer name")
@click.option('--out_file', '-o', default=CONFIG["out_file"],
              help=HELP["out_file"])
@click.option('--out_format', '-of', default=CONFIG["out_format"],
              help=HELP["out_format"])
@click.option('--new_layer_name', '-nln', help="Output layer name")
@click.option('--n_processes', '-p', default=CONFIG["n_processes"],
              help="Number of parallel processing threads to utilize")
def overlay(in_file, in_layer, out_file, out_format, new_layer_name, n_processes):
    """Intersect layer with designatedlands"""
    # load in_file to postgres
    db = pgdb.connect(CONFIG["db_url"], schema="public")
    if not in_layer:
        in_layer = fiona.listlayers(in_file)[0]
    if not new_layer_name:
        new_layer_name = in_layer
    ogr2pg(db, in_file, in_layer=in_layer, out_layer=new_layer_name)
    # pull distinct tiles iterable into a list
    tiles = [t for t in db["tiles"].distinct('map_tile')]
    # uncomment and adjust for debugging a specific tile
    # tiles = [t for t in tiles if t[:4] == '092K']
    info("Intersecting %s with %s" % ('designatedlands', new_layer_name))
    intersect(db, "designatedlands",
              new_layer_name, new_layer_name + "_overlay", n_processes, tiles)
    # dump result to file
    info("Dumping intersect to file %s " % out_file)
    pg2ogr(CONFIG["db_url"],
           "SELECT * FROM %s_overlay" % new_layer_name,
           out_format,
           out_file,
           new_layer_name,
           geom_type="MULTIPOLYGON")


@cli.command()
@click.option('--out_table', '-o', default=CONFIG["out_table"],
              help=HELP["out_table"])
@click.option('--out_file', '-o', default=CONFIG["out_file"],
              help=HELP["out_file"])
@click.option('--out_format', '-of', default=CONFIG["out_format"],
              help=HELP["out_format"])
def dump(out_table, out_file, out_format):
    """Dump output designated lands layer to gdb
    """
    info('Dumping %s to %s' % (out_table, out_file))
    sql = """SELECT
               designation, category, bc_boundary, map_tile, geom
             FROM {t}
          """.format(t=out_table)
    pg2ogr(CONFIG["db_url"], sql, out_format, out_file, out_table,
           geom_type="MULTIPOLYGON")


@cli.command()
@click.option('--source_csv', '-s', default=CONFIG["source_csv"],
              type=click.Path(exists=True), help=HELP['csv'])
@click.option('--email', help=HELP['email'])
@click.option('--dl_path', default=CONFIG["source_data"],
              type=click.Path(exists=True), help=HELP['dl_path'])
@click.option('--out_table', default=CONFIG["out_table"],
              help=HELP['out_table'])
@click.option('--out_file', default=CONFIG["out_file"], help=HELP['out_file'])
@click.option('--out_format', '-of', default=CONFIG["out_format"],
              help=HELP["out_format"])
def run_all(source_csv, email, dl_path, out_table, out_file, out_format):
    """ Run complete designated lands job
    """
    create_db()
    load(source_csv, email, dl_path)
    process(source_csv, out_table)
    dump(out_file, out_format)


if __name__ == '__main__':
    cli()
