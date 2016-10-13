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

import requests
import click
import fiona

import bcdata
import pgdb


# --------------------------------------------
# Change default data paths/filenames etc here
# --------------------------------------------
CONFIG = {"downloads": "downloads",
          "source_csv": "sources.csv",
          "out_table": "conservation_lands",
          "out_shp": "conservation_lands.shp",
          "schema": "conservation_lands"}
# --------------------------------------------
# --------------------------------------------

CHUNK_SIZE = 1024

logging.basicConfig(level=logging.INFO)


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
    source_list = [row for row in csv.DictReader(open(path, 'rb'))]
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
    # check that the extracted download isn't already in tmp
    if not email:
        email = os.environ["BCDATA_EMAIL"]
    if not email:
        raise Exception("Set BCDATA_EMAIL environment variable")
    if gdb and os.path.exists(os.path.join(dl_path, gdb)):
        return os.path.join(dl_path, gdb)
    else:
        order_id = bcdata.create_order(url, email)
        if not order_id:
            raise Exception("Failed to create DWDS order")
        # download and extract the order
        download = bcdata.download_order(order_id)
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
    command = ["ogr2ogr",
               "--config PG_USE_COPY YES",
               "-t_srs "+t_srs,
               "-f PostgreSQL",
               """PG:'host={h} user={u} dbname={db} password={pwd}' \
               """.format(h=db.host,
                          u=db.user,
                          db=db.database,
                          pwd=db.password),
               "-lco OVERWRITE=YES",
               "-lco SCHEMA={schema}".format(schema=schema),
               "-lco GEOMETRY_NAME=geom",
               "-dim 2",
               "-nln "+out_layer,
               "-nlt PROMOTE_TO_MULTI",
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


def pg2shp(db, sql, out_shp, t_srs='EPSG:3005'):
    """Dump a PostGIS query to shapefile
    """
    command = ["ogr2ogr",
               "-t_srs "+t_srs,
               out_shp,
               """PG:'host={h} user={u} dbname={db} password={pwd}' \
               """.format(h=db.host,
                          u=db.user,
                          db=db.database,
                          pwd=db.password),
               "-lco OVERWRITE=YES",
               "-sql "+sql]
    info('Dumping query to %s' % out_shp)
    subprocess.call(" ".join(command), shell=True)


@click.group()
def cli():
    pass


@cli.command()
@click.option('--source_csv', '-s', default=CONFIG["source_csv"],
              type=click.Path(exists=True))
@click.option('--email',
              prompt=True,
              default=lambda: os.environ.get('BDATA_EMAIL', ''))
@click.option('--dl_path', default=CONFIG["downloads"],
              type=click.Path(exists=True))
@click.option('--alias', '-a')
def download(source_csv, email, dl_path, alias):
    """Download data, load to postgres
    """
    # create download path if it doesn't exist
    make_sure_path_exists(dl_path)

    sources = read_csv(source_csv)

    # only try and download data where scripted download is supported
    sources = [s for s in sources if s["manual_download"] != 'T']

    # if provided an alias, only download that single layer
    if alias:
        sources = [s for s in sources if s["alias"] == alias]

    # connect to postgres database, create working schema if it doesn't exist
    db = pgdb.connect()
    db.create_schema(CONFIG["schema"])

    for source in sources:
        info("Downloading %s" % source["alias"])

        # handle BCGW downloads
        if urlparse(source["url"]).hostname == 'catalogue.data.gov.bc.ca':
            gdb = source["layer_in_file"].split(".")[1]+".gdb"
            file = download_bcgw(source["url"], dl_path, gdb=gdb)
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
               out_layer="src_"+source["alias"],
               schema=CONFIG["schema"],
               sql=source["query"])


@cli.command()
@click.option('--source_csv', '-s', default=CONFIG["source_csv"],
              type=click.Path(exists=True))
@click.option('-dl_path', default=CONFIG["downloads"])
def load_manual_downloads(source_csv, dl_path):
    """Load manually downloaded data to postgres
    """
    db = pgdb.connect()
    # create schema if it doesn't exist
    db.create_schema(CONFIG["schema"])
    sources = read_csv(source_csv)
    sources = [s for s in sources if s["manual_download"] == 'T']
    for source in sources:
        file = os.path.join(dl_path, source["file_in_url"])
        layer = source["layer_in_file"]
        ogr2pg(db,
               file,
               in_layer=layer,
               out_layer="src_"+source["alias"],
               schema=CONFIG["schema"],
               sql=source["query"])


@cli.command()
@click.option('--source_csv', '-s', default=CONFIG["source_csv"],
              type=click.Path(exists=True))
def clean(source_csv):
    """Clean/validate all input data
    """
    db = pgdb.connect()
    for source in read_csv(source_csv):
        # for testing, just use automated downloads
        if source["manual_download"] != 'T':
            info("Cleaning %s" % source["alias"])
            # Make things easier to find by ordering the layers by hierarchy #
            # Any layers that aren't given a hierarchy number will have c00_
            # as the layer name prefix
            hierarchy = str(source["hierarchy"]).zfill(2)
            clean_layer = "c"+hierarchy+"_"+source["alias"]
            clean_table = CONFIG["schema"]+"."+clean_layer

            # Drop the table if it already exists
            db[clean_table].drop()
            lookup = {"out_table": clean_table,
                      "layer": clean_layer,
                      "source": CONFIG["schema"]+".src_"+source["alias"]}
            sql = db.build_query(db.queries["clean"], lookup)
            db.execute(sql)


@cli.command()
@click.option('--source_csv', '-s', default=CONFIG["source_csv"],
              type=click.Path(exists=True))
def pre_process(source_csv):
    """Unsupported
    """
    """
    Loop through layers where source["preprocess"] has a value, execute
    the action specified by that value

    For example, for the layer with these values:

    source["preprocess_operation"] = 'clip'
    source["preprocess_layer_alias"] = 'mk_boundary'

      - clip the (cleaned) input layer by 'mk_boundary' to temp table
      - drop the existing cleaned layer
      - rename the clipped layer to previously existing cleaned layer
    """
    pass


@cli.command()
@click.option('--source_csv', '-s', default=CONFIG["source_csv"],
              type=click.Path(exists=True))
@click.option('--out_table', '-o', default=CONFIG["out_table"])
def process(source_csv, out_table):
    """Create output conservation lands layer
    """
    db = pgdb.connect()
    db[CONFIG["schema"]+"."+out_table].drop()
    out_table = CONFIG["schema"]+"."+out_table
    db.execute(db.build_query(db.queries['create_output'],
                              {"output": out_table}))
    # use only sources that have a hierarchy number
    sources = [s for s in read_csv(source_csv) if s['hierarchy']]

    # for testing, just use automated downloads
    sources = [s for s in sources if s["manual_download"] != 'T']

    for source in sources:
        info("Inserting %s into output" % source["alias"])
        hierarchy = str(source["hierarchy"]).zfill(2)
        in_table = CONFIG["schema"]+".c"+hierarchy+"_"+source["alias"]
        sql = db.build_query(db.queries["populate_output"],
                             {"input": in_table,
                              "output": out_table})
        db.execute(sql)
    # cleanup - merge national parks
    sql = """UPDATE {table}
             SET category = 'park_national'
             WHERE category LIKE 'park_national%'
          """.format(table=out_table)
    db.execute(sql)


@cli.command()
@click.option('--out_shape', '-o', default=CONFIG["out_shp"])
def dump(out_shape):
    """Dump output conservation lands layer to shp
    """
    db = pgdb.connect()
    sql = """SELECT
               ROW_NUMBER() OVER() as id,
               category,
               geom
             FROM (SELECT category, ST_Union(geom)
                   FROM {s}.output
                   GROUP BY category) as foo
          """.format(s=CONFIG["schema"])
    pg2shp(db, sql, out_shape)


@cli.command()
@click.option('--source_csv', '-s', default=CONFIG["source_csv"],
              type=click.Path(exists=True))
@click.option('--email',
              prompt=True,
              default=lambda: os.environ.get('BDATA_EMAIL', ''))
@click.option('--dl_path', default=CONFIG["downloads"],
              type=click.Path(exists=True))
@click.option('--out_table', default=CONFIG["out_table"])
@click.option('--out_shape', default=CONFIG["out_shp"])
def run_all(source_csv, email, dl_path, out_table, out_shape):
    """ Run complete conservation lands job
    """
    download(source_csv, email, dl_path)
    load_manual_downloads(source_csv, dl_path)
    clean(source_csv)
    pre_process(source_csv)
    process(source_csv, out_table)
    dump(out_shape)


if __name__ == '__main__':
    cli()
