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

import logging
from lsst.daf.butler import Butler, DatasetRef, FileDataset
from lsst.daf.butler.registry import DatasetTypeError, MissingCollectionError
from lsst.pipe.base import Instrument

LOGGER = logging.getLogger(__name__)


class RseButler:
    """Object that wraps an instance of a Butler with files in an RSE

    Parameters
    ----------
    repo : `str`
        Butler repo location
    instrument : `str`
        instrument registered for this Butler
    """

    def __init__(self, repo: str, instrument: str):
        try:
            self.butlerConfig = Butler.makeRepo(repo)
        except FileExistsError:
            self.butlerConfig = repo

        self.butler = self.createButler(instrument)

    def createButler(self, instrument: str) -> Butler:
        """Create a Butler for an instrument

        Parameters
        ----------
        instrument : `str`
            instrument to register
        """
        opts = dict(writeable=True)
        butler = Butler(self.butlerConfig, **opts)
        instr = Instrument.from_string(instrument)
        instr.register(butler.registry)

        return butler

    def createEntry(self, butler_file: str, sidecar: str) -> FileDataset:
        """Create a FileDatset with sidecar information

        Parameters
        ----------
        butler_file: `str`
            full uri to butler file location
        sidecar: `str`
            JSON representation of the 'sidecar' metadata
        """
        ref = DatasetRef.from_json(sidecar, registry=self.butler.registry)
        fds = FileDataset(butler_file, ref)
        return fds

    def ingest(self, datasets: list):
        """Ingest a list of Datasets

        Parameters
        ----------
        datasets : `list`
            List of Datasets
        """
        ingested = False
        while not ingested:
            try:
                self.butler.ingest(*datasets, transfer="direct")
                print("ingest succeeded")
                ingested = True
            except DatasetTypeError:
                dst_set = set()
                for dataset in datasets:
                    for dst in {ref.datasetType for ref in dataset.refs}:
                        dst_set.add(dst)
                for dst in dst_set:
                    self.butler.registry.registerDatasetType(dst)
            except MissingCollectionError:
                run_set = set()
                for dataset in datasets:
                    for run in {ref.run for ref in dataset.refs}:
                        run_set.add(run)
                for run in run_set:
                    self.butler.registry.registerRun(run)