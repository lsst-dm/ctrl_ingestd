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
from lsst.daf.butler import Butler, DatasetType, FileDataset
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
        entries : `list[Entry]`
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
            data_type_dict[data_type].append(entry)

        if DataType.ZIP_FILE in data_type_dict:
            self._ingest_zip(data_type_dict[DataType.ZIP_FILE])
        if DataType.RAW_FILE in data_type_dict:
            self._ingest(data_type_dict[DataType.RAW_FILE], "direct", True)
        if DataType.DATA_PRODUCT in data_type_dict:
            self._ingest(data_type_dict[DataType.DATA_PRODUCT], "auto", False)
        if DataType.DIM_FILE in data_type_dict:
            self._ingest_dim(data_type_dict[DataType.DIM_FILE])

    def _get_non_registered_datasets(self, datasets: list[FileDataset]) -> list[FileDataset]:
        """Return the list of datasets which are unknown to this butler among
        the provided list of datasets.

        Parameters
        ----------
        datasets : `list`
            List of FileDataset objects.
        """
        non_registered: list[FileDataset] = []
        for dataset in datasets:
            for dataset_id in {ref.id for ref in dataset.refs}:
                if self.butler.get_dataset(dataset_id) is None:
                    non_registered.append(dataset)
                else:
                    LOGGER.info(f'file "{dataset.path}" is already ingested')

        return non_registered

    def _ingest_dim(self, entries: list):
        dim_files = [e.get_data() for e in entries]
        for dim_file in dim_files:
            try:
                LOGGER.info("importing dimension file %s", dim_file)
                self.butler.import_(filename=dim_file)
                LOGGER.info("imported %s", dim_file)
            except Exception as e:
                LOGGER.info(e)

    def _ingest_zip(self, entries: list):
        zip_files = [e.get_data() for e in entries]
        for zip_file in zip_files:
            try:
                self.butler.ingest_zip(zip_file)
                LOGGER.info("ingested %s", zip_file)
            except Exception as e:
                LOGGER.info(e)

    def _ingest_raw(self, entries: list):
        files = [e.file_to_ingest for e in entries]
        self.task.run(files)

    def _registerDatasetTypes(self, datasets: list[FileDataset]) -> None:
        dst_set: set[DatasetType] = set()
        for dataset in datasets:
            dst_set.update({ref.datasetType for ref in dataset.refs})

        for dst in dst_set:
            LOGGER.info("registering dataset type: %s", dst.name)
            self.butler.registry.registerDatasetType(dst)

    def _registerRuns(self, datasets: list[FileDataset]) -> None:
        run_set: set[str] = set()
        for dataset in datasets:
            run_set.update({ref.run for ref in dataset.refs})

        for run in run_set:
            LOGGER.info("registering %s", run)
            self.butler.registry.registerRun(run)

    def _ingest(self, entries: list, transfer, retry_as_raw):
        """Ingest

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

        dataset_count = len(datasets)
        maximum_attempts = dataset_count + 2

        remaining_attempts = maximum_attempts

        while not completed:
            error = "Unknown"
            try:
                remaining_attempts -= 1
                self.butler.ingest(*datasets, transfer=transfer)
                LOGGER.debug("ingest succeeded")
                for dataset in datasets:
                    LOGGER.info(f"ingested: {dataset.path}")
                completed = True
            except DatasetTypeError:
                LOGGER.debug("DatasetTypeError")
                self._registerDatasetTypes(datasets)
                error = "'need to register dataset type'"
            except MissingCollectionError:
                LOGGER.debug("MissingCollectionError")
                self._registerRuns(datasets)
                error = "'missing collections'"
            except Exception as e:
                if retry_as_raw:
                    LOGGER.debug(f"{e} - defaulting to raw ingest task")
                    self._ingest_raw(entries)
                    completed = True
                else:
                    LOGGER.warning(e)

            if not completed:
                pending_datasets = self._get_non_registered_datasets(datasets)
                if not pending_datasets:
                    LOGGER.info("all pending datasets ingested")
                    return
                elif remaining_attempts > 0:
                    datasets = pending_datasets
                    LOGGER.info(
                        "datasets left to ingest after %s error: %d out of %d",
                        error,
                        len(pending_datasets),
                        dataset_count,
                    )
                else:
                    LOGGER.info(
                        "could not ingest %d/%d datasets but reached limit of %d ingest attempts",
                        len(pending_datasets),
                        dataset_count,
                        maximum_attempts,
                    )
                    for dataset in pending_datasets:
                        try:
                            self._single_ingest(dataset, transfer, retry_as_raw)
                        except RuntimeError as re:
                            continue
                    # XXX - probably raise an exception here
                    return
        LOGGER.info("all %d datasets ingested", dataset_count)

    def _single_ingest(self, dataset, transfer, retry_as_raw):
        LOGGER.debug("called")
        still_attempting = True
        datasets = [dataset]
        while still_attempting:
            still_attempting = False
            try:
                self.butler.ingest(*datasets, transfer=transfer)
                LOGGER.info("ingested: %s", dataset.path)
            except DatasetTypeError:
                LOGGER.debug("DatasetTypeError")
                self._registerDatasetTypes(datasets)
                still_attempting = True
            except MissingCollectionError:
                LOGGER.debug("MissingCollectionError")
                self._registerRuns(datasets)
                still_attempting = True
            except Exception as e:
                if retry_as_raw:
                    LOGGER.debug(f"{e} - defaulting to raw ingest task")
                    self._ingest_raw([dataset.path])  # XYZZY - fix this
                else:
                    LOGGER.warning(e)
            if not still_attempting:
                LOGGER.info("couldn't ingest %s", dataset.path)
                raise RuntimeError(f"couldn't ingest {dataset.path}")

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
            return f"{e.__cause__!s}"
        else:
            return f"{e.__cause__!s};  {cause}"
