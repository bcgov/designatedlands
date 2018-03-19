import os
import subprocess
import tempfile
from urllib.parse import urlparse

import click

import pgdata

from designatedlands import download
from designatedlands import util
from designatedlands.config import config

HELP = {
    "cfg": 'Path to designatedlands config file',
    "alias": "The 'alias' key for the source of interest",
}


@click.group()
def cli():
    pass


@cli.command()
def create_db():
    """Create a fresh database
    """
    util.log('Creating database %s' % config['db_url'])
    pgdata.create_db(config["db_url"])
    db = pgdata.connect(config["db_url"])
    db.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    # the pgxn extension does not work on windows
    # note to the user to add lostgis functions manually with provided
    # .bat file as a reference
    if os.name == 'posix':
        db.execute("CREATE EXTENSION IF NOT EXISTS lostgis")
    else:
        util.log(
            'Remember to add required lostgis functions to your new database',
            level=30,
        )
        util.log('See scripts\lostgis_windows.bat as a guide', level=30)


@cli.command()
@click.option('--alias', '-a', help=HELP['alias'])
@click.option('--force_download', default=False, help='Force fresh download')
def load(alias, force_download):
    """Download data, load to postgres
    """
    db = pgdata.connect(config["db_url"])
    sources = util.read_csv(config["source_csv"])
    # filter sources based on optional provided alias and ignoring excluded
    if alias:
        sources = [s for s in sources if s["alias"] == alias]
    sources = [s for s in sources if s["exclude"] != 'T']
    # process sources where automated downloads are avaiable
    load_commands = []
    for source in [s for s in sources if s["manual_download"] != 'T']:
        # handle BCGW downloads
        if urlparse(source["url"]).hostname == 'catalogue.data.gov.bc.ca':
            gdb = source["layer_in_file"].split(".")[1].strip() + ".gdb"
            file = os.path.join(config["dl_path"], gdb)
            layer = download.get_layer_name(file, source["layer_in_file"])
            # download only if layer is not already there or if forced
            if not os.path.exists(file) or force_download:
                file, layer = download.download_bcgw(
                    source["url"],
                    config["dl_path"],
                    email=config["email"],
                    gdb=gdb,
                    layer=source["layer_in_file"],
                )
        # handle all other downloads (zipfiles only)
        else:
            if os.path.exists(
                os.path.join(config['dl_path'], source["alias"])
            ):
                file = os.path.join(
                    config['dl_path'], source["alias"], source['file_in_url']
                )
                layer = download.get_layer_name(file, source["layer_in_file"])
            else:
                download_cache = os.path.join(
                    tempfile.gettempdir(), "dl_cache"
                )
                if not os.path.exists(download_cache):
                    os.makedirs(download_cache)
                file, layer = download.download_non_bcgw(
                    source['url'],
                    config['dl_path'],
                    source['alias'],
                    source['file_in_url'],
                    download_cache,
                )
        load_commands.append(
            db.ogr2pg(
                file,
                in_layer=layer,
                out_layer=source["input_table"],
                sql=source["query"],
                cmd_only=True,
            )
        )
    # process manually downloaded sources
    for source in [s for s in sources if s["manual_download"] == 'T']:
        file = os.path.join(config['dl_path'], source["file_in_url"])
        if not os.path.exists(file):
            raise Exception(file + " does not exist, download it manually")

        layer = download.get_layer_name(file, source["layer_in_file"])
        load_commands.append(
            db.ogr2pg(
                file,
                in_layer=layer,
                out_layer=source["input_table"],
                sql=source["query"],
                cmd_only=True,
            )
        )
    # run all ogr commands in parallel
    util.log('Loading source data to database.')
    # https://stackoverflow.com/questions/14533458/python-threading-multiple-bash-subprocesses
    processes = [subprocess.Popen(cmd, shell=True) for cmd in load_commands]
    for p in processes:
        p.wait()
    # create tiles layer
    util.log('Creating tiles layer')
    db.execute(db.queries["create_tiles"])


@cli.command()
@click.option(
    '--resume', '-r', help='hierarchy number at which to resume processing'
)
@click.option(
    '--force_preprocess',
    is_flag=True,
    default=False,
    help="Force re-preprocessing of input data",
)
@click.option(
    '--tiles', default=None, help="Comma separated list of tiles to process"
)
def process(config, resume, force_preprocess, tiles):
    """Create output designatedlands tables
    """
    config = util.read_config(config)
    out_table = config['out_table'].lower()
    db = pgdata.connect(config["db_url"], schema="public")
    # run required preprocessing, tile, attempt to clean inputs
    preprocess(db, config['source_csv'], force=force_preprocess)
    tile_sources(db, config['source_csv'], force=force_preprocess)
    clean_and_agg_sources(db, config['source_csv'], force=force_preprocess)
    # translate tile arg to 20k tiles (allows passing values like '093')
    if tiles:
        arg_tiles = []
        for tile_token in tiles.split(","):
            sql = """
                  SELECT DISTINCT map_tile
                  FROM tiles
                  WHERE map_tile LIKE %s"""
            tiles20 = [
                r[0] for r in db.query(sql, (tile_token + '%',)).fetchall()
            ]
            arg_tiles = arg_tiles + tiles20
    else:
        arg_tiles = None
    # create target tables if not resuming from a bailed process
    if not resume:
        # create output tables
        db.execute(
            db.build_query(
                db.queries["create_outputs_prelim"], {"table": out_table}
            )
        )
    # filter sources - use only non-exlcuded sources with hierarchy > 0
    sources = [
        s
        for s in util.read_csv(config['source_csv'])
        if s['hierarchy'] != 0 and s["exclude"] != 'T'
    ]
    # To create output table with overlaps, combine all source data
    # (tiles argument does not apply, we could build a tile query string but
    # it seems unnecessary)
    for source in sources:
        util.log(
            "Inserting %s into preliminary output overlap table" %
            source["tiled_table"]
        )
        sql = db.build_query(
            db.queries["populate_output_overlaps"],
            {
                "in_table": source["tiled_table"],
                "out_table": out_table + "_overlaps",
            },
        )
        db.execute(sql)
    # To create output table with no overlaps, more processing is required
    # In case of bailing during tests/development, `resume` option is available
    # to enable resumption of processing at specified hierarchy number
    if resume:
        p_sources = [s for s in sources if int(s["hierarchy"]) >= int(resume)]
    else:
        p_sources = sources
    # The tiles layer will fill in gaps between sources (so all BC is included
    # in output). To do this, first match schema of tiles to other sources
    db.execute("ALTER TABLE tiles ADD COLUMN IF NOT EXISTS id integer")
    db.execute("UPDATE tiles SET id = tile_id")
    db.execute("ALTER TABLE tiles ADD COLUMN IF NOT EXISTS designation text")
    # Next, add simple tiles layer definition to sources list
    p_sources.append({"cleaned_table": "tiles", "category": None})
    # iterate through all sources
    for source in p_sources:
        sql = db.build_query(
            db.queries["populate_output"],
            {
                "in_table": source["cleaned_table"],
                "out_table": out_table + "_prelim",
            },
        )
        # determine which specified tiles are present in source layer
        src_tiles = set(
            get_tiles(db, source["cleaned_table"], tile_table='a00_tiles_20k')
        )
        if arg_tiles:
            tiles = set(arg_tiles) & src_tiles
        else:
            tiles = src_tiles
        if tiles:
            util.log(
                "Inserting %s into preliminary output table" %
                source["cleaned_table"]
            )
            # for testing, run only one process and report on tile
            if n_processes == 1:
                for tile in tiles:
                    util.log(tile)
                    db.execute(sql, (tile + "%",) * 2)
            else:
                func = partial(parallel_tiled, db.url, sql, n_subs=2)
                pool = multiprocessing.Pool(processes=n_processes)
                pool.map(func, tiles)
                pool.close()
                pool.join()
    # create marine-terrestrial layer
    if 'bc_boundary' not in db.tables:
        create_bc_boundary(db, n_processes)
    util.log('Cutting %s with marine-terrestrial definition' % out_table)
    intersect(
        db, out_table + "_prelim", "bc_boundary", out_table, n_processes, tiles
    )
    tidy_designations(db, sources, "cleaned_table", out_table)
    tidy_designations(db, sources, "cleaned_table", out_table + "_overlaps")


@cli.command()
@click.argument('in_file', type=click.Path(exists=True))
@click.option('--in_layer', '-l', help="Input layer name")
@click.option(
    '--dump_file',
    is_flag=True,
    default=False,
    help="Dump to file (as specified by out_file and out_format)",
)
@click.option('--new_layer_name', '-nln', help="Name of overlay output layer")
def overlay(config, in_file, in_layer, dump_file, new_layer_name):
    """Intersect layer with designatedlands
    """
    config = util.read_config(config)
    # load in_file to postgres
    db = pgdata.connect(config['db_url'], schema="public")
    if not in_layer:
        in_layer = fiona.listlayers(in_file)[0]
    if not new_layer_name:
        new_layer_name = in_layer[:63]  # Maximum table name length is 63
    out_layer = new_layer_name[:50] + "_overlay"
    db.ogr2pg(in_file, in_layer=in_layer, out_layer=new_layer_name)
    # pull distinct tiles iterable into a list
    tiles = [t for t in db["tiles"].distinct('map_tile')]
    # uncomment and adjust for debugging a specific tile
    # tiles = [t for t in tiles if t[:4] == '092K']
    util.log("Intersecting %s with %s" % (config['out_table'], new_layer_name))
    intersect(
        db,
        config['out_table'],
        new_layer_name,
        out_layer,
        config['n_processes'],
        tiles,
    )
    # dump result to file
    if dump_file:
        util.log("Dumping intersect to file %s " % config['out_file'])
        dump(out_layer, config['out_file'], config['out_format'])


@cli.command()
def dump(config):
    """Dump output designatedlands table to file
    """
    config = util.read_config(config)
    db = pgdata.connect(config["db_url"], schema="public")
    util.log('Dumping %s to %s' % (config['out_table'], config['out_file']))
    columns = [c for c in db[config['out_table']].columns if c != 'geom']
    ogr_sql = """SELECT {cols},
                  st_snaptogrid(geom, .001) as geom
                FROM {t}
                WHERE designation IS NOT NULL
             """.format(
        cols=",".join(columns), t=config['out_table']
    )
    util.log(ogr_sql)
    db = pgdata.connect(config["db_url"])
    db.pg2ogr(
        ogr_sql,
        config['out_format'],
        config['out_file'],
        config['out_table'],
        geom_type="MULTIPOLYGON",
    )


@cli.command()
def run_all(config):
    """ Run complete designated lands job
    """
    create_db(config)
    load(config)
    process(config)
    dump(config)


if __name__ == '__main__':
    cli()
