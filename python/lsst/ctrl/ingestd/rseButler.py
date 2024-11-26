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
from timeit import default_timer as timer

from lsst.daf.butler import Butler, DatasetRef, DatasetType, FileDataset
from lsst.daf.butler.registry import DatasetTypeError, MissingCollectionError

LOGGER = logging.getLogger(__name__)


class RseButler:
    """Object that wraps an instance of a Butler with files in an RSE

    Parameters
    ----------
    configuration : `str`
        Butler configuration
    """

    def __init__(self, configuration: str) -> None:
        LOGGER.info(f"instantiating Butler from configuration: {configuration}")
        self._butler_config: str = configuration
        self._butler: Butler = Butler(self._butler_config, writeable=True)

    def create_entry(self, file_name: str, sidecar: dict) -> FileDataset:
        """Create a FileDataset with sidecar information.

        Parameters
        ----------
        file_name: `str`
            name of the existing file to ingest into the Butler repo.
            It can be an absolute URL or a path relative to the Butler
            datastore root.
        sidecar: `dict`
            dictionary of the 'sidecar' metadata
        """
        ref = DatasetRef.from_json(sidecar, registry=self._butler.registry)
        return FileDataset(file_name, ref)

    def _register_dataset_types(self, datasets: list[FileDataset]) -> None:
        """Register into this butler the dataset types of a list of
        FileDataset objects.

        Parameters
        ----------
        datasets : `list`
            List of FileDataset objects.
        """
        dataset_types: set[DatasetType] = set()
        for dataset in datasets:
            dataset_types = dataset_types.union({ref.datasetType for ref in dataset.refs})

        for dataset_type in dataset_types:
            LOGGER.info(f'registering dataset type "{dataset_type.name}"')
            self._butler.registry.registerDatasetType(dataset_type)

    def _register_runs(self, datasets: list[FileDataset]) -> None:
        """Register into this butler the runs of a list of FileDataset
        objects.

        Parameters
        ----------
        datasets : `list`
            List of FileDataset objects.
        """
        runs: set[str] = set()
        for dataset in datasets:
            runs = runs.union({ref.run for ref in dataset.refs})

        for run in runs:
            LOGGER.info(f'registering run "{run}"')
            self._butler.registry.registerRun(run)

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
                if self._butler.get_dataset(dataset_id) is None:
                    non_registered.append(dataset)
                else:
                    LOGGER.info(f'file "{dataset.path}" is already ingested')

        return non_registered

    def ingest(self, datasets: list[FileDataset], transfer="auto") -> None:
        """Ingest a list of FileDataset objects into this Butler.

        Parameters
        ----------
        datasets : `list`
            List of FileDataset to ingest.
        """
        if not datasets:
            return

        start = timer()
        dataset_count = len(datasets)
        LOGGER.info(f'starting ingestion of {dataset_count} datasets into butler "{self._butler_config}"')

        # Set the maximum number of ingestion attempts to the number of
        # datasets to ingest + 1 attempt for registration of dataset types +
        # 1 attempt for registration of runs.
        remaining_attempts = dataset_count + 2

        completed = False
        while not completed:
            try:
                # Attempt ingesting all remaining datasets at once.
                remaining_attempts -= 1
                self._butler.ingest(*datasets, transfer=transfer)
                for dataset in datasets:
                    LOGGER.info(f"ingested file: {dataset.path}")
                completed = True

            except DatasetTypeError:
                # The dataset type of at least one dataset is unknown to the
                # Butler. Register all the dataset types first and try again
                # ingesting the remaining datasets.
                self._register_dataset_types(datasets)

            except MissingCollectionError:
                # The collection for at least one dataset does not exist in
                # the target Butler repo. Create all the necessary collections
                # first and try again ingesting the remaining datasets.
                self._register_runs(datasets)

            except Exception as e:
                # Attempting to ingest a Butler dataset which already exists
                # in the repo raises.
                LOGGER.warning(e)

            if not completed:
                # Not all datasets of this batch were successfully ingested.
                # Identify which datasets are not already in the Butler repo
                # and try ingesting again only those, since ingesting an
                # existing dataset with the same dataset id raises.
                pending_datasets = self._get_non_registered_datasets(datasets)
                if not pending_datasets:
                    # No remaining datasets to ingest
                    completed = True
                elif remaining_attempts > 0:
                    datasets = pending_datasets
                    LOGGER.info(
                        "remaining datasets to ingest after recovering from error:"
                        f" {len(datasets)} out of {dataset_count}"
                    )
                else:
                    LOGGER.warning(
                        f"could not ingest {len(pending_datasets)}/{dataset_count} datasets "
                        " but reached the maximum number of ingestion attempts"
                    )
                    return

        elapsed = timer() - start
        LOGGER.info(f"successfully ingested {dataset_count} datasets in {elapsed:.3f}s")
