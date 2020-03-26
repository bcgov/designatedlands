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
import sys

import click
import fiona
from cligj import verbose_opt, quiet_opt

import pgdata

from designatedlands import util
from designatedlands import DesignatedLands
from designatedlands import main


def get_logger(verbose, quiet):
    verbosity = verbose - quiet
    log_level = max(10, 20 - 10 * verbosity)  # default to INFO log level
    logging.basicConfig(
        stream=sys.stderr,
        level=log_level,
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    )
    return logging.getLogger(__name__)


@click.group()
def cli():
    pass


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@click.option("--alias", "-a", help="The 'alias' key for the source of interest")
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite any existing output, force fresh download",
)
@verbose_opt
@quiet_opt
def download(config_file, alias, overwrite, verbose, quiet):
    """Download data, load to postgres
    """
    get_logger(verbose, quiet)
    DL = DesignatedLands(config_file)
    DL.download(alias=alias, overwrite=overwrite)


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@click.option("--alias", "-a", help="The 'alias' key for the source of interest")
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite any existing output, force fresh download",
)
@verbose_opt
@quiet_opt
def preprocess(config_file, alias, overwrite, verbose, quiet):
    get_logger(verbose, quiet)
    DL = DesignatedLands(config_file)
    DL.preprocess(alias=alias)


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@verbose_opt
@quiet_opt
def tidy(config_file, verbose, quiet):
    get_logger(verbose, quiet)
    DL = DesignatedLands(config_file)
    DL.tidy()


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@verbose_opt
@quiet_opt
def cleanup(config_file, verbose, quiet):
    get_logger(verbose, quiet)
    DL = DesignatedLands(config_file)
    DL.cleanup()


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@click.option(
    "--overwrite", is_flag=True, default=False, help="Overwrite any existing outputs",
)
@verbose_opt
@quiet_opt
def merge(config_file, overwrite, verbose, quiet):
    """Clean inputs and merge into output 'overlaps' table
    """
    logger = get_logger(verbose, quiet)
    DL = DesignatedLands(config_file, alias)

    # if alias provided, only process that layer
    if alias:
        DL.sources = [s for s in DL.sources if s["alias"] == alias]
    if not sources:
        raise ValueError("Alias %s does not exist" % alias)


"""
#@click.option("--resume", "-r", help="hierarchy number at which to resume processing")

    main.clean_and_agg_sources(db, config["source_csv"], force=force_preprocess)

    # parse the list of tiles
    tilelist = main.parse_tiles(db, tiles)
    # create target tables if not resuming from a bailed process
    if not resume:
        # create output tables
        db.execute(
            db.build_query(
                db.queries["create_outputs_prelim"], {"table": config["out_table"]}
            )
        )
    # filter sources - use only non-exlcuded sources with hierarchy > 0
    sources = [
        s
        for s in util.read_csv(config["source_csv"])
        if s["hierarchy"] != 0 and s["exclude"] != "T"
    ]
    # To create output table with overlaps, combine all source data
    # (tiles argument does not apply, we could build a tile query string but
    # it seems unnecessary)
    for source in sources:
        logger.info(
            "Inserting %s into preliminary output overlap table" % source["tiled_table"]
        )
        sql = db.build_query(
            db.queries["populate_output_overlaps"],
            {
                "in_table": source["tiled_table"],
                "out_table": config["out_table"] + "_overlaps_prelim",
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
                "out_table": config["out_table"] + "_prelim",
            },
        )
        # determine which specified tiles are present in source layer
        src_tiles = set(
            main.get_tiles(db, source["cleaned_table"], tile_table="tiles")
        )
        if tilelist:
            tiles = set(tilelist) & src_tiles
        else:
            tiles = src_tiles
        if tiles:
            logger.info(
                "Inserting %s into preliminary output table" % source["cleaned_table"]
            )
            # for testing, run only one process and report on tile
            if config["n_processes"] == 1:
                for tile in tiles:
                    util.log(tile)
                    db.execute(sql, (tile + "%",) * 2)
            else:
                func = partial(main.parallel_tiled, db.url, sql, n_subs=2)
                pool = multiprocessing.Pool(processes=config["n_processes"])
                pool.map(func, tiles)
                pool.close()
                pool.join()
        else:
            logger.info("No tiles to process")
    # create marine-terrestrial layer
    if "bc_boundary" not in db.tables:
        main.create_bc_boundary(db, config["n_processes"])

    # overlay output tables with marine-terrestrial definition
    for table in [config["out_table"], config["out_table"] + "_overlaps"]:
        logger.info("Cutting %s with marine-terrestrial definition" % table)
        main.intersect(
            db, table + "_prelim", "bc_boundary", table, config["n_processes"], tiles
        )

    util.tidy_designations(db, sources, "cleaned_table", config["out_table"])
    util.tidy_designations(
        db, sources, "tiled_table", config["out_table"] + "_overlaps"
    )

"""


@cli.command()
@click.argument("in_file", type=click.Path(exists=True))
@click.argument("config_file", type=click.Path(exists=True), required=False)
@click.option("--in_layer", "-l", help="Input layer name")
@click.option(
    "--dump_file", is_flag=True, default=False, help="Dump to file (out_file in .cfg)"
)
@click.option("--new_layer_name", "-nln", help="Name of overlay output layer")
@verbose_opt
@quiet_opt
def overlay(in_file, config_file, in_layer, dump_file, new_layer_name, verbose, quiet):
    """Intersect layer with designatedlands
    """
    config = util.read_config(config_file)
    logger = get_logger(verbose, quiet)

    # load in_file to postgres
    db = pgdata.connect(config["db_url"], schema="public")
    if not in_layer:
        in_layer = fiona.listlayers(in_file)[0]
    if not new_layer_name:
        new_layer_name = in_layer[:63]  # Maximum table name length is 63
    out_layer = new_layer_name[:50] + "_overlay"
    db.ogr2pg(in_file, in_layer=in_layer, out_layer=new_layer_name)
    # pull distinct tiles iterable into a list
    tiles = [t for t in db["tiles"].distinct("map_tile")]
    # uncomment and adjust for debugging a specific tile
    # tiles = [t for t in tiles if t[:4] == '092K']
    logger.info("Intersecting %s with %s" % (config["out_table"], new_layer_name))
    main.intersect(
        db, config["out_table"], new_layer_name, out_layer, config["n_processes"], tiles
    )
    # dump result to file
    if dump_file:
        logger.info("Dumping intersect to file %s " % config["out_file"])
        dump(out_layer, config["out_file"], config["out_format"])


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@click.option(
    "--overlaps",
    is_flag=True,
    default=False,
    help="Dump output _overlaps table to file",
)
@click.option(
    "--aggregate", is_flag=True, default=False, help="Aggregate over tile boundaries"
)
@verbose_opt
@quiet_opt
def dump(config_file, overlaps, aggregate, verbose, quiet):
    """Dump output designatedlands table to file
    """
    config = util.read_config(config_file)
    util.log_config(verbose, quiet)
    logger = logging.getLogger(__name__)
    if aggregate:
        if overlaps:
            util.log("ignoring --overlaps flag")
        main.dump_aggregate(config, "designatedlands_agg")
    else:
        if overlaps:
            config["out_table"] = config["out_table"] + "_overlaps"
        db = pgdata.connect(config["db_url"], schema="public")
        logger.info("Dumping %s to %s" % (config["out_table"], config["out_file"]))
        columns = [
            c
            for c in db[config["out_table"]].columns
            if c != "geom" and "prelim" not in c
        ]
        ogr_sql = """SELECT {cols},
                    st_collectionextract(st_safe_repair(st_snaptogrid(geom, .001)), 3) as geom
                    FROM {t}
                    WHERE designation IS NOT NULL
                """.format(
            cols=",".join(columns), t=config["out_table"]
        )
        logger.info(ogr_sql)
        db = pgdata.connect(config["db_url"])
        db.pg2ogr(
            ogr_sql,
            config["out_format"],
            config["out_file"],
            config["out_table"],
            geom_type="MULTIPOLYGON",
        )


if __name__ == "__main__":
    cli()
