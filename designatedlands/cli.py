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
from pathlib import Path

import pgdata

from designatedlands import util
from designatedlands import DesignatedLands
from designatedlands import main


def set_log_level(verbose, quiet):
    verbosity = verbose - quiet
    log_level = max(10, 20 - 10 * verbosity)  # default to INFO log level
    logging.basicConfig(
        stream=sys.stderr,
        level=log_level,
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    )


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
    set_log_level(verbose, quiet)
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
    """Create tiles layer and preprocess sources where required"""
    set_log_level(verbose, quiet)
    DL = DesignatedLands(config_file)
    DL.preprocess(alias=alias)
    DL.create_bc_boundary()


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@verbose_opt
@quiet_opt
def process_vector(config_file, verbose, quiet):
    """Create vector designation/restriction layers"""
    set_log_level(verbose, quiet)
    DL = DesignatedLands(config_file)
    DL.tidy()
    DL.restrictions()


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@verbose_opt
@quiet_opt
def process_raster(config_file, verbose, quiet):
    """Create raster designation/restriction layers"""
    set_log_level(verbose, quiet)
    DL = DesignatedLands(config_file)
    DL.rasterize()
    DL.overlay_rasters()


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@verbose_opt
@quiet_opt
def dump(config_file, verbose, quiet):
    """Dump output tables to file"""
    set_log_level(verbose, quiet)
    DL = DesignatedLands(config_file)
    # overwrite output gpkg if it exists
    out_path = Path(DL.config["out_path"])/"designatedlands.gpkg"
    if out_path.exists():
        out_path.unlink()
    for table in ["designatedlands", "forest_restriction", "og_restriction", "mine_restriction"]:
        DL.db.pg2ogr(
            f"SELECT * FROM designatedlands.{table}",
            "GPKG",
            str(out_path),
            table,
            geom_type="MULTIPOLYGON",
        )


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
    set_log_level(verbose, quiet)
    DL = DesignatedLands(config_file)

    if not in_layer:
        in_layer = fiona.listlayers(in_file)[0]
    if not new_layer_name:
        new_layer_name = in_layer[:63]  # Maximum table name length is 63
    out_layer = new_layer_name[:50] + "_overlay"
    DL.db.ogr2pg(in_file, in_layer=in_layer, out_layer=new_layer_name)
    # pull distinct tiles iterable into a list
    tiles = [t for t in DL.db["tiles"].distinct("map_tile")]
    DL.intersect(
        "designatedlands", new_layer_name, out_layer, DL.config["n_processes"], tiles
    )
    # dump result to file
    if dump_file:
        dump(out_layer, DL.config["out_file"], DL.config["out_format"])


@cli.command()
@click.argument("config_file", type=click.Path(exists=True), required=False)
@verbose_opt
@quiet_opt
def cleanup(config_file, verbose, quiet):
    """Remove temporary tables
    """
    set_log_level(verbose, quiet)
    DL = DesignatedLands(config_file)
    DL.cleanup()


if __name__ == "__main__":
    cli()
