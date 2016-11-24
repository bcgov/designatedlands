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
from multiprocessing import Pool
from functools import partial

import requests
import click
import fiona

import bcdata
import pgdb


# --------------------------------------------
# Change default database/paths/filenames etc here
# --------------------------------------------
CONFIG = {
    "source_data": "source_data",
    "source_csv": "sources.csv",
    "out_table": "conservation_lands",
    "out_shp": "conservation_lands.shp",
    # sqlalchemy postgresql database url
    # http://docs.sqlalchemy.org/en/latest/core/engines.html#postgresql
    "db_url":
    "postgresql://postgres:postgres@localhost:5432/conservationlands",
    "n_processes": 3
    }
# --------------------------------------------
# --------------------------------------------

CHUNK_SIZE = 1024

logging.basicConfig(level=logging.INFO)

HELP = {
  "csv": 'path to a csv that lists all input data and sources',
  "email": 'a valid email address to use for DataBC downloads',
  "dl_path": 'path to folder for saving downloaded data',
  "alias": "the 'alias' key identifing the layer of interest, from source csv",
  "out_shape": "Name of output conservation lands shapefile",
  "out_table": 'name of output conservation lands postgresql table'}


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


def get_tiles(db, table, tile_table="tiles_250k"):
    """Return a list of all tiles intersecting the given table's geom
    """
    sql = """SELECT DISTINCT b.map_tile
             FROM {table} a
             INNER JOIN {tile_table} b ON st_intersects(b.geom, a.geom)
             ORDER BY map_tile
          """.format(table=table,
                     tile_table=tile_table)
    return [r[0] for r in db.query(sql)]


def clean(source):
    db = pgdb.connect(CONFIG["db_url"], schema="public")
    info("Cleaning %s" % source["alias"])
    db[source["clean_table"]].drop()
    lookup = {"out_table": source["clean_table"],
              "src_table": source["src_table"]}
    sql = db.build_query(db.queries["clean"], lookup)
    db.execute(sql)


def preprocess(db, source_csv, n_processes):
    """
    Before running the main processing job:
      - create comprehensive tiling layer
      - preprocess sources as specified in source_csv
    """
    sources = read_csv(source_csv)

    # create tile layer
    db.execute(db.queries["create_tiles"])

    # for all conservation lands sources:
    # - union/merge polygons
    # - create new table name prefixed with hierarchy
    # - retain just a single column (category), value equivalent to table name
    clean_sources = [s for s in sources
                     if s["exclude"] != 'T' and s['hierarchy'] != 0]
    for source in clean_sources:
        info("Tiling, dissolving and validating %s" % source["alias"])
        db[source["clean_table"]].drop()
        lookup = {"out_table": source["clean_table"],
                  "src_table": source["src_table"]}
        sql = db.build_query(db.queries["clean"], lookup)
        db.execute(sql)

    # rather than looping, enable multiprocessing of the clean job
    #pool = Pool(processes=n_processes)
    #pool.map(clean, clean_sources)
    #pool.close()
    #pool.join()

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


def run_overlay(sql, tile):
    """Call the query that populates the output layer via multiprocessing
    """
    db = pgdb.connect(CONFIG["db_url"], schema="public")
    db.execute(sql, (tile+"%", tile))


def postprocess(db, sources, out_table):
    """Postprocess the output conservation lands table
    """
    # add rollup column by creating lookup table from source.csv
    lookup_data = [dict(alias=s["clean_table"],
                        rollup=s["rollup"])
                   for s in sources if s["rollup"]]
    # create lookup table
    db["rollup_lookup"].drop()
    db.execute("""CREATE TABLE rollup_lookup
                  (id SERIAL PRIMARY KEY, alias TEXT, rollup TEXT)""")
    db["rollup_lookup"].insert(lookup_data)

    # add rollup column
    if "rollup" not in db[out_table].columns:
        db.execute("""ALTER TABLE {t}
                      ADD COLUMN rollup TEXT
                   """.format(t=out_table))

    # populate rollup column from lookup
    db.execute("""UPDATE {t} AS o
                  SET rollup = lut.rollup
                  FROM rollup_lookup AS lut
                  WHERE o.category = lut.alias
               """.format(t=out_table))

    # Remove national park names from the national park tags
    sql = """UPDATE {t}
             SET category = 'c01_park_national'
             WHERE category LIKE 'c01_park_national%'
          """.format(t=out_table)
    db.execute(sql)


def pg2shp(db, sql, out_shp, t_srs='EPSG:3005'):
    """Dump a PostGIS query to shapefile
    """
    command = ['ogr2ogr',
               '-t_srs '+t_srs,
               '-lco OVERWRITE=YES',
               out_shp,
               '''PG:"host={h} user={u} dbname={db} password={pwd}"'''.format(
                  h=db.host,
                  u=db.user,
                  db=db.database,
                  pwd=db.password),
               '-sql "'+sql+'"']
    info('Dumping data to %s' % out_shp)
    subprocess.call(" ".join(command), shell=True)


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
    sources = read_csv(source_csv)

    # only try and download data where scripted download is supported
    sources = [s for s in sources if s["manual_download"] != 'T']

    # ignore the layers that are flagged as excluded
    sources = [s for s in sources if s["exclude"] != 'T']

    # if provided an alias, only download that single layer
    if alias:
        sources = [s for s in sources if s["alias"] == alias]

    db = pgdb.connect(CONFIG["db_url"])
    for source in sources:
        info("Downloading %s" % source["alias"])

        # handle BCGW downloads
        if urlparse(source["url"]).hostname == 'catalogue.data.gov.bc.ca':
            gdb = source["layer_in_file"].split(".")[1]+".gdb"
            file = download_bcgw(source["url"], dl_path, email=email, gdb=gdb)
            # read the layer name from the gdb
            layer = fiona.listlayers(file)[0]

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
            layer = source["layer_in_file"]

        # load downloaded data to postgres
        ogr2pg(db,
               file,
               in_layer=layer,
               out_layer=source["alias"],
               sql=source["query"])

    # Load manually downloaded data to postgres
    sources = read_csv(source_csv)
    sources = [s for s in sources if s["manual_download"] == 'T']
    for source in sources:
        file = os.path.join(dl_path, source["file_in_url"])
        if not os.path.exists(file):
            raise Exception(file+" does not exist, download it manually")
        layer = source["layer_in_file"]
        ogr2pg(db,
               file,
               in_layer=layer,
               out_layer=source["alias"],
               sql=source["query"])


@cli.command()
@click.option('--source_csv', '-s', default=CONFIG["source_csv"],
              type=click.Path(exists=True), help=HELP['csv'])
@click.option('--out_table', '-o', default=CONFIG["out_table"],
              help=HELP["out_table"])
@click.option('--resume', '-r',
              help='hierarchy number at which to resume processing')
@click.option('--no_preprocess', is_flag=True)
@click.option('--n_processes', '-p', default=CONFIG["n_processes"],
              help="Number of parallel processing threads to utilize")
def process(source_csv, out_table, resume, no_preprocess, n_processes):
    """Create output conservation lands table
    """
    db = pgdb.connect(CONFIG["db_url"], schema="public")

    # run any required pre-processing
    # (no_preprocess flag is for development)
    if not no_preprocess:
        preprocess(db, source_csv, n_processes)

    # create output table if not resuming from a bailed process
    if not resume:
        # create output table
        db[out_table].drop()
        db.execute(db.build_query(db.queries['create_output'],
                                  {"table": out_table}))

    # filter sources - use only non-exlcuded sources with hierarchy > 0
    sources = [s for s in read_csv(source_csv)
               if s['hierarchy'] != 0 and s["exclude"] != 'T']

    # in case of bailing during tests/development, specify resume option to
    # resume processing at specified hierarchy number
    if resume:
        sources = [s for s in sources if int(s["hierarchy"]) >= int(resume)]

    # iterate through all sources
    for source in sources:
        info("Inserting %s into output" % source["alias"])
        hierarchy = str(source["hierarchy"]).zfill(2)
        in_table = "c"+hierarchy+"_"+source["alias"]
        sql = db.build_query(db.queries["populate_output"],
                             {"in_table": in_table,
                              "out_table": out_table})
        # process 250k tiles in parallel
        tiles = get_tiles(db, in_table)
        func = partial(run_overlay, sql)
        pool = Pool(processes=n_processes)
        pool.map(func, tiles)
        pool.close()
        pool.join()

    # clean up the output
    postprocess(db, sources, out_table)


@cli.command()
@click.option('--out_table', '-o', default=CONFIG["out_table"],
              help=HELP["out_table"])
@click.option('--out_shape', '-o', default=CONFIG["out_shp"],
              help=HELP["out_shape"])
def dump(out_table, out_shape):
    """Dump output conservation lands layer to shp
    """
    db = pgdb.connect(CONFIG["db_url"])
    sql = """SELECT category, rollup, geom
             FROM {s}.{t}
          """.format(s=CONFIG["schema"], t=out_table)
    pg2shp(db, sql, out_shape)


@cli.command()
@click.option('--source_csv', '-s', default=CONFIG["source_csv"],
              type=click.Path(exists=True), help=HELP['csv'])
@click.option('--email', help=HELP['email'])
@click.option('--dl_path', default=CONFIG["source_data"],
              type=click.Path(exists=True), help=HELP['dl_path'])
@click.option('--out_table', default=CONFIG["out_table"],
              help=HELP['out_table'])
@click.option('--out_shape', default=CONFIG["out_shp"], help=HELP['out_shape'])
def run_all(source_csv, email, dl_path, out_table, out_shape):
    """ Run complete conservation lands job
    """
    create_db()
    load(source_csv, email, dl_path)
    process(source_csv, out_table)
    dump(out_shape)


if __name__ == '__main__':
    cli()
