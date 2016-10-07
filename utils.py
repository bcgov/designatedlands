# https://github.com/OpenBounds/Processing/blob/master/utils.py

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

import bcdata

CHUNK_SIZE = 1024

logging.basicConfig(level=logging.INFO)


def get_files(path):
    """Returns an iterable containing the full path of all files in the
    specified path.
    :param path: string
    :yields: string
    """
    if os.path.isdir(path):
        for (dirpath, dirnames, filenames) in os.walk(path):
            for filename in filenames:
                if not filename[0] == '.':
                    yield os.path.join(dirpath, filename)
    else:
        yield path


def read_csv(path):
    """Returns list of dicts from file.
    :param path: string
    :returns: dict
    """
    return [row for row in csv.DictReader(open(path, 'rb'))]


def make_sure_path_exists(path):
    """Make directories in path if they do not exist.
    Modified from http://stackoverflow.com/a/5032238/1377021
    :param path: string
    """
    try:
        os.makedirs(path)
    except:
        pass


def get_path_parts(path):
    """Splits a path into parent directories and file.
    :param path: string
    """
    return path.split(os.sep)


def download_bcgw(url, email=None, gdb=None):
    """Download BCGW data using DWDS
    """
    # check that the extracted download isn't already in tmp
    if not email:
        email = os.environ["BCDATA_EMAIL"]
    if not email:
        raise Exception("Set BCDATA_EMAIL environment variable")
    if gdb and os.path.exists(os.path.join(tempfile.gettempdir(), gdb)):
        info("Returning %s from local cache" % gdb)
        return os.path.join(tempfile.gettempdir(), gdb)
    else:
        order_id = bcdata.create_order(url, email)
        if not order_id:
            raise Exception("Failed to create DWDS order")
        # download and extract the order
        download = bcdata.download_order(order_id)
        # move the downloaded .gdb up to temp folder
        up_one = os.path.dirname(os.path.dirname(download))
        out_gdb = os.path.split(download)[1]
        shutil.copytree(download, os.path.join(up_one, out_gdb))
        return os.path.join(up_one, out_gdb)


def download(url):
    """Download a file to $TMP
    :param url: string
    """
    info('Downloading', url)

    parsed_url = urlparse(url)

    urlfile = parsed_url.path.split('/')[-1]
    _, extension = os.path.split(urlfile)

    fp = tempfile.NamedTemporaryFile('wb', suffix=extension, delete=False)

    download_cache = os.getenv("DOWNLOAD_CACHE")
    cache_path = None
    if download_cache is not None:
        cache_path = os.path.join(download_cache,
                                  hashlib.sha224(url).hexdigest())
        if os.path.exists(cache_path):
            info(cache_path)
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


def extract(fp, source_filetype, source_filename, layer):
    """Unzip the archive, return path to specified file
    (this presumes that we already know the name of the desired file)
    """
    info('Extracting', fp.name)
    unzip_dir = tempfile.mkdtemp(suffix='_'+source_filetype)
    zipped_file = get_compressed_file_wrapper(fp.name)
    zipped_file.extractall(unzip_dir)
    zipped_file.close()
    return os.path.join(unzip_dir, source_filename)


def info(*strings):
    logging.info(' '.join(strings))


def error(*strings):
    logging.error(' '.join(strings))


class ZipCompatibleTarFile(tarfile.TarFile):
    """Wrapper around TarFile to make it more compatible with ZipFile"""
    def infolist(self):
        members = self.getmembers()
        for m in members:
            m.filename = m.name
        return members

    def namelist(self):
        return self.getnames()

ARCHIVE_FORMAT_ZIP = "zip"
ARCHIVE_FORMAT_TAR_GZ = "tar.gz"
ARCHIVE_FORMAT_TAR_BZ2 = "tar.bz2"


def get_compressed_file_wrapper(path):
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
    """Load a layer to provided pgdb database connection using OGR2OGR

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
        command.insert(4, '-sql "SELECT * FROM %s WHERE %s" -dialect SQLITE' % (in_layer, sql))
        # remove layer name, it is ignored in combination with sql
        command.pop()
    info('Loading %s to %s' % (out_layer, db.url))
    subprocess.call(" ".join(command), shell=True)
