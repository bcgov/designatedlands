import os
import urlparse

import click
import fiona

import pgdb
import utils

import config


@click.group()
def cli():
    pass


@cli.command()
@click.option('--source_csv', '-s', default=config.csv,
              type=click.Path(exists=True))
@click.option('--email',
              prompt=True,
              default=lambda: os.environ.get('BDATA_EMAIL', ''))
@click.option('--dl_path', default="download_cache",
              type=click.Path(exists=True))
@click.option('--alias', '-a')
def download(source_csv, email, dl_path, alias, force):
    """Download data, load to postgres
    """
    # create download path if it doesn't exist
    utils.make_sure_path_exists(dl_path)

    sources = utils.read_csv(source_csv)

    # only try and download data where scripted download is supported
    sources = [s for s in sources if s["download_supported"] == 'T']

    # if provided an alias, only download that single layer
    if alias:
        sources = [s for s in sources if s["alias"] == alias]

    # connect to postgres database, create working schema if it doesn't exist
    db = pgdb.connect()
    db.create_schema(config.schema)

    for source in sources:
        utils.info("Downloading %s" % source["alias"])

        # handle BCGW downloads
        if urlparse.urlparse(source["url"]).hostname == 'catalogue.data.gov.bc.ca':
            gdb = source["layer_in_file"].split(".")[1]+".gdb"
            file = utils.download_bcgw(source["url"], dl_path, gdb=gdb)
            # read the layer name from the gdb
            layer = fiona.listlayers(file)[0]

        # handle all other downloads
        else:
            fp = utils.download(source['url'], dl_path)
            file = utils.extract(fp,
                                 source['file_in_url'],
                                 source['layer_in_file'],
                                 dl_path)
            layer = source["layer_in_file"]

        # load downloaded data to postgres
        utils.ogr2pg(db,
                     file,
                     in_layer=layer,
                     out_layer="src_"+source["alias"],
                     schema=config.schema,
                     sql=source["query"])


@cli.command()
@click.option('--source_csv', '-s', default=config.csv,
              type=click.Path(exists=True))
@click.option('-dl_path', default="download_cache")
def process_manual_downloads(source_csv, dl_path):
    """Load manually downloaded data to postgres
    """
    db = pgdb.connect()
    # create schema if it doesn't exist
    db.create_schema(config.schema)
    sources = utils.read_csv(source_csv)
    sources = [s for s in sources if s["download_supported"] == 'F']
    for source in sources:
        file = os.path.join(dl_path, source["file_in_url"])
        layer = source["layer_in_file"]
        utils.ogr2pg(db,
                     file,
                     in_layer=layer,
                     out_layer="src_"+source["alias"],
                     schema=config.schema,
                     sql=source["query"])


@cli.command()
@click.option('--source_csv', '-s', default=config.csv,
              type=click.Path(exists=True))
def clean(source_csv):
    """Clean/validate all input data
    """
    db = pgdb.connect()
    for source in utils.read_csv(source_csv):
        if source["download_supported"] == 'T':
            utils.info("Cleaning %s" % source["alias"])
            # Make things easier to find by ordering the layers by hierarchy #
            # Any layers that aren't given a hierarchy number will have c00_
            # as the layer name prefix
            hierarchy = str(source["hierarchy"]).zfill(2)
            out_layer = "c"+hierarchy+"_"+source["alias"]
            out_table = config.schema+"."+out_layer

            # Drop the table if it already exists
            db[out_table].drop()
            lookup = {"out_table": out_table,
                      "layer": out_layer,
                      "source": config.schema+".src_"+source["alias"]}
            sql = db.build_query(db.queries["clean"], lookup)
            db.execute(sql)


@cli.command()
@click.option('--source_csv', '-s', default=config.csv,
              type=click.Path(exists=True))
def pre_process(source_csv):
    """
    Unsupported (TODO)

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
@click.option('--source_csv', '-s', default=config.csv,
              type=click.Path(exists=True))
@click.option('--out_table', '-o', default="conservation_lands")
def process(source_csv, out_table):
    """Create output conservation lands layer
    """
    db = pgdb.connect()
    db[config.schema+".output"].drop()
    sources = [s for s in utils.read_csv(source_csv)
               if s["download_supported"] == 'T']
    out_table = config.schema+"."+out_table
    db.execute(db.build_query(db.queries['create_output'],
                              {"output": out_table}))
    # loop through all the data
    sources = [s for s in sources if s['hierarchy']]

    # for testing, just use automated downloads
    sources = [s for s in sources if s["download_supported"] == 'T']

    for source in sources:
        utils.info("Inserting %s into output" % source["alias"])
        hierarchy = str(source["hierarchy"]).zfill(2)
        in_table = config.schema+".c"+hierarchy+"_"+source["alias"]
        sql = db.build_query(db.queries["populate_output"],
                             {"input": in_table,
                              "output": out_table})
        click.echo(sql)
        db.execute(sql)
    # cleanup - merge national parks
    sql = """UPDATE {table}
             SET category = 'park_national'
             WHERE category LIKE 'park_national%'
          """.format(table=out_table)
    db.execute(sql)


@cli.command()
@click.option('--out_shape', '-o', default=config.output_shp)
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
          """.format(s=config.schema)
    utils.pg2shp(db, sql, out_shape)


@cli.command()
@click.option('--source_csv', '-s', default=config.csv,
              type=click.Path(exists=True))
@click.option('--email',
              prompt=True,
              default=lambda: os.environ.get('BDATA_EMAIL', ''))
@click.option('--dl_path', default="download_cache",
              type=click.Path(exists=True))
@click.option('--out_table', default="conservation_lands")
@click.option('--out_shape', default=config.output_shp)
def run_all(source_csv, email, dl_path, out_table, out_shape):
    """ Run complete conservation lands job
    """
    download(source_csv, email, dl_path)
    process_manual_downloads(source_csv, dl_path)
    clean(source_csv)
    pre_process(source_csv)
    process(source_csv, out_table)
    dump(out_shape)


if __name__ == '__main__':
    cli()
