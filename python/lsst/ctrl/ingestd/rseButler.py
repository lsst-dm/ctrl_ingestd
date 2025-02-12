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

from lsst.ctrl.ingestd.entries.dataType import DataType
from lsst.daf.butler import Butler
from lsst.daf.butler.registry import DatasetTypeError, MissingCollectionError
from lsst.obs.base.ingest import RawIngestConfig, RawIngestTask

LOGGER = logging.getLogger(__name__)


class RseButler:
    """Object that wraps an instance of a Butler with files in an RSE

    Parameters
    ----------
    repo : `str`
        Butler repo location
    """

    def __init__(self, repo: str):

        self.butler = Butler(repo, writeable=True)
        cfg = RawIngestConfig()
        cfg.transfer = "direct"
        self.task = RawIngestTask(
            config=cfg,
            butler=self.butler,
            on_success=self.on_success,
            on_ingest_failure=self.on_ingest_failure,
            on_metadata_failure=self.on_metadata_failure,
        )

    def ingest(self, entries: list):
        """ingest a list of datasets

        Parameters
        ----------
        entries : `list`
            List of Entry
        """

        #
        # group entries by data type, so they can be run in batches
        #
        data_type_dict = {}
        LOGGER.debug(f"{entries=}")
        for entry in entries:
            data_type = entry.get_data_type()
            if data_type not in data_type_dict:
                data_type_dict[data_type] = []
            LOGGER.debug(f"adding {data_type=}, {entry=}")
            data_type_dict[data_type].append(entry)

        if DataType.RAW_FILE in data_type_dict:
            self._ingest(data_type_dict[DataType.RAW_FILE], "direct", True)
        if DataType.DATA_PRODUCT in data_type_dict:
            self._ingest(data_type_dict[DataType.DATA_PRODUCT], "auto", False)

    def _ingest_raw(self, entries: list):
        """ingest using raw Task

        Parameters
        ----------
        entries : `list`
            List of Entry
        """
        try:
            files = [e.file_to_ingest for e in entries]
            LOGGER.debug(f"{files=}")
            self.task.run(files)
        except Exception as e:
            LOGGER.warning(e)

    def _ingest(self, entries: list, transfer, retry_as_raw):
        """ingest data with dataset refs, optionally 
        retrying using RawIngestTask

        Parameters
        ----------
        entries : `list`
            List of Entry
        transfer: `str`
            Butler transfer type
        retry_as_raw: `bool`
            on ingest failure, retry using RawIngestTask
        """
        LOGGER.debug(f"{entries=}")
        completed = False

        datasets = [e.get_data() for e in entries]

        while not completed:
            try:
                self.butler.ingest(*datasets, transfer=transfer)
                LOGGER.debug("ingest succeeded")
                for dataset in datasets:
                    LOGGER.debug(f"ingested: {dataset.path}")
                completed = True
            except DatasetTypeError:
                LOGGER.debug("DatasetTypeError")
                dst_set = set()
                for dataset in datasets:
                    for dst in {ref.datasetType for ref in dataset.refs}:
                        dst_set.add(dst)
                for dst in dst_set:
                    self.butler.registry.registerDatasetType(dst)
            except MissingCollectionError:
                LOGGER.debug("MissingCollectionError")
                run_set = set()
                for dataset in datasets:
                    for run in {ref.run for ref in dataset.refs}:
                        run_set.add(run)
                for run in run_set:
                    self.butler.registry.registerRun(run)
            except Exception as e:
                if retry_as_raw:
                    LOGGER.warning(f"{e} - defaulting to raw ingest task")
                    self._ingest_raw(entries)
                completed = True

    def on_success(self, datasets):
        """Callback used on successful ingest. Used to transmit
        successful data ingestion status

        Parameters
        ----------
        datasets: `list`
            list of DatasetRefs
        """
        for dataset in datasets:
            LOGGER.info("file %s successfully ingested", dataset.path)

    def on_ingest_failure(self, exposures, exc):
        """Callback used on ingest failure. Used to transmit
        unsuccessful data ingestion status

        Parameters
        ----------
        exposures: `RawExposureData`
            exposures that failed in ingest
        exc: `Exception`
            Exception which explains what happened

        """
        for f in exposures.files:
            filename = f.filename
            cause = self.extract_cause(exc)
            LOGGER.info(f"{filename}: ingest failure: {cause}")

    def on_metadata_failure(self, filename, exc):
        """Callback used on metadata extraction failure. Used to transmit
        unsuccessful data ingestion status

        Parameters
        ----------
        filename: `ButlerURI`
            ButlerURI that failed in ingest
        exc: `Exception`
            Exception which explains what happened
        """
        cause = self.extract_cause(exc)
        LOGGER.info(f"{filename}: metadata failure: {cause}")

    def extract_cause(self, e):
        """extract the cause of an exception

        Parameters
        ----------
        e : `BaseException`
            exception to extract cause from

        Returns
        -------
        s : `str`
            A string containing the cause of an exception
        """
        if e.__cause__ is None:
            return f"{e}"
        cause = self.extract_cause(e.__cause__)
        if cause is None:
            return f"{str(e.__cause__)}"
        else:
            return f"{str(e.__cause__)};  {cause}"
