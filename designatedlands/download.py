import re
import os
import hashlib
import requests
import shutil
import tarfile
import tempfile
import urllib.request
from urllib.parse import urlparse
import zipfile

import fiona
import bcdata

from designatedlands import util

CHUNK_SIZE = 1024


def get_layer_name(file, layer_name):
    """ Find the layer in the file source that best matches provided layer_name
    """
    layers = fiona.listlayers(file)
    # replace the . with _ in WHSE objects
    if re.match("^WHSE_", layer_name):
        layer_name = re.sub("\\.", "_", layer_name)

    if len(layers) > 1:
        if layer_name not in layers:
            # look for  layername_polygon
            if layer_name + '_polygon' in layers:
                layer = layer_name + '_polygon'
            else:
                raise Exception("cannot find layer name")
        else:
            layer = layer_name
    else:
        layer = layers[0]
    return layer


def download_bcgw(url, dl_path, email, gdb=None, layer=None):
    """Download BCGW data using DWDS
    """
    # get just the package name from the url
    package = os.path.split(urlparse(url).path)[1]

    download = bcdata.download(package, email)
    if not download:
        raise Exception("Failed to create DWDS order")
    # move the download to specified dl_path, deleting if it already exists
    if gdb:
        out_gdb = gdb
    else:
        out_gdb = os.path.split(download)[1]
    if os.path.exists(os.path.join(dl_path, out_gdb)):
        shutil.rmtree(os.path.join(dl_path, out_gdb))
    shutil.copytree(download, os.path.join(dl_path, out_gdb))

    if layer:
        layer_name = get_layer_name(os.path.join(dl_path, out_gdb), layer)
    else:
        layer_name = None
    return (os.path.join(dl_path, out_gdb), layer_name)


def download_non_bcgw(url, dl_path, alias, source_filename, source_layer,
                      download_cache=None):
    """
    Download a zipfile to location specified
    Modified from https://github.com/OpenBounds/Processing/blob/master/utils.py
    """

    parsed_url = urlparse(url)

    urlfile = parsed_url.path.split('/')[-1]
    _, extension = os.path.split(urlfile)

    fp = tempfile.NamedTemporaryFile('wb', suffix=extension, delete=False)

    cache_path = None
    if download_cache is not None:
        cache_path = os.path.join(
            download_cache,
            hashlib.sha224(url.encode('utf-8')).hexdigest())
        if os.path.exists(cache_path):
            info("Returning %s from local cache at %s" % (url, cache_path))
            fp.close()
            shutil.copy(cache_path, fp.name)
            return fp

    info('Downloading', url)
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

    if cache_path:
        if not os.path.exists(download_cache):
            os.makedirs(download_cache)
        shutil.copy(fp.name, cache_path)

    # extract zipfile (this is the only supported non-bcgw file format)
    out_file = extract(fp, dl_path, alias, source_filename)

    if source_layer:
        layer_name = get_layer_name(out_file, source_layer)
    else:
        layer_name = None
    return (out_file, layer_name)


def extract(fp, dl_path, alias, source_filename):
    """
    Unzip the archive, return path to specified file
    (this presumes that we already know the name of the desired file)
    Modified from https://github.com/OpenBounds/Processing/blob/master/utils.py
    """
    info('Extracting', fp.name)
    unzip_dir = util.make_sure_path_exists(os.path.join(dl_path, alias))
    info(unzip_dir)
    zipped_file = get_compressed_file_wrapper(fp.name)
    zipped_file.extractall(unzip_dir)
    zipped_file.close()
    return os.path.join(unzip_dir, source_filename)


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
