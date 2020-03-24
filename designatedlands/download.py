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

import fiona


CHUNK_SIZE = 1024
LOG = logging.getLogger(__name__)


def download_non_bcgw(url, path, filename, layer=None, force_download=False):
    """
    Download and extract a zipfile to unique location
    Modified from https://github.com/OpenBounds/Processing/blob/master/utils.py
    """
    # create a unique name for downloading and unzipping, this ensures a given
    # url will only get downloaded once
    out_folder = os.path.join(path, hashlib.sha224(url.encode("utf-8")).hexdigest())
    out_file = os.path.join(out_folder, filename)
    if force_download and os.path.exists(out_folder):
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
