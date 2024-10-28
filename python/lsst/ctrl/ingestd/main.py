# This file is part of ctrl_ingestd
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import argparse
import logging
import os

import lsst.log as lsstlog
from lsst.ctrl.ingestd.ingestd import IngestD

lsstlog.usePythonLogging()

CTRL_INGESTD_CONFIG = "CTRL_INGESTD_CONFIG"


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Listens to Kafka events on successfully replicated files and ingest them into a local butler."
    )
    parser.add_argument(
        "-c",
        "--config",
        metavar="FILE",
        type=str,
        dest="config",
        required=False,
        help=f"Configuration file for this daemon in YAML format. Default: value of the environment variable {CTRL_INGESTD_CONFIG}.",
    )

    parser.add_argument("-D", "--debug", action="store_true", help="Set log level to DEBUG. Default: INFO.")
    return parser.parse_args()


def main():
    args = parse_arguments()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(levelname) -10s %(asctime)s.%(msecs)03dZ %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config_filename = args.config or os.environ.get(CTRL_INGESTD_CONFIG)
    if config_filename is None:
        raise FileNotFoundError(
            f'environment variable "{CTRL_INGESTD_CONFIG}" is not set and command line option "--config" was not specified'
        )

    IngestD(config_filename)


if __name__ == "__main__":
    main()
