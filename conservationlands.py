import os
import urlparse
import tempfile
import getpass

import fiona

import pgdb
import utils

DATALIST = "sources_test.csv"

SCHEMA = 'conservation_lands'
GRID_SOURCE = 'nts-250k-grid-digital-baseline-mapping-at-1-250-000-nts'
GRID_GDB = "NTS_250K_GRID.gdb"
GRID_LAYER = "nts_250k_grid"


def download(db):
    # if not already set, set download cache to temp file
    if not os.getenv("DOWNLOAD_CACHE"):
        os.environ["DOWNLOAD_CACHE"] = tempfile.gettempdir()
    if not os.getenv("BCDATA_EMAIL"):
        prompt = "Enter an email to use for downloading BC data:"
        os.environ["BCDATA_EMAIL"] = getpass.getpass(prompt)

    # connect to postgres database, create working schema if it doesn't exist
    db.create_schema(SCHEMA)
    # download and convert a convenient grid by which we can tile processing
    if GRID_LAYER not in db.tables_in_schema(SCHEMA):
        file = utils.download_bcgw(GRID_SOURCE, gdb=GRID_GDB)
        layer = fiona.listlayers(file)[0]
        utils.ogr2pg(db, file, in_layer=layer, out_layer=GRID_LAYER,
                     schema=SCHEMA)

    # download all supported source data
    sources = utils.read_csv(DATALIST)
    for source in sources:
        if source["alias"] not in db.tables_in_schema(SCHEMA):
            # read file extension to determine file type, defaulting to gdb
            if source["file_in_url"]:
                file_type = source["file_in_url"][-3:]
            else:
                file_type = "gdb"
            # for testing, only try to download files we can get at right now
            if source["download_supported"] == 'T':
                utils.info("Downloading %s" % source["alias"])
                # BCGW layers are prefixed by the catalog url
                dwds = 'catalogue.data.gov.bc.ca'
                if urlparse.urlparse(source["url"]).hostname == dwds:
                    gdb = source["layer_in_file"].split(".")[1]+".gdb"
                    file = utils.download_bcgw(source["url"], gdb=gdb)
                    # read the layer name from the gdb
                    layer = fiona.listlayers(file)[0]
                else:
                    fp = utils.download(source['url'])
                    file = utils.extract(fp,
                                         file_type,
                                         source['file_in_url'],
                                         source['layer_in_file'])
                    layer = source["layer_in_file"]
                # load data to postgres
                utils.ogr2pg(db,
                             file,
                             in_layer=layer,
                             out_layer=source["alias"],
                             schema="conservation_lands",
                             sql=source["query"])


def clean(db):
    """Clean all input data, making sure polys are valid, doesn't overlap, etc
    """
    for source in utils.read_csv(DATALIST):
        if source["download_supported"] == 'T':
            alias = source["alias"]
            utils.info("Cleaning %s" % alias)

            # for testing, drop the table if it already exists
            db[SCHEMA+"."+alias+"_c"].drop()

            lookup = {"out_table": SCHEMA+"."+alias+"_c",
                      "alias": alias,
                      "source": SCHEMA+"."+alias}
            sql = db.build_query(db.queries["clean"], lookup)
            db.execute(sql)


def process(db):
    """
    Create output layer, then iterate through each input adding records
    that do not overlap anything that has already been added
    """
    db[SCHEMA+".output"].drop()
    sources = [s for s in utils.read_csv(DATALIST)
               if s["download_supported"] == 'T']
    db.execute(db.build_query(db.queries['create_output'],
                              {"output": SCHEMA+".output"}))
    # loop through all the data
    for source in sources:
        utils.info("Inserting %s into output" % source["alias"])
        sql = db.build_query(db.queries["populate_output"],
                             {"input": SCHEMA+"."+source["alias"]+"_c",
                             "output": SCHEMA+".output"})
        db.execute(sql)


def merge_national_parks(db):
    """National Parks come from several files, merge all into a single table
    """
    sql = """UPDATE {table}
             SET category = 'park_national'
             WHERE category LIKE 'park_national%'
          """.format(table=SCHEMA+".output")
    db.execute(sql)

db = pgdb.connect()
#download(db)
#clean(db)
#process(db)
merge_national_parks(db)