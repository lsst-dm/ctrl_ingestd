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
        sidecar: `dict`
            dictionary of the 'sidecar' metadata
        """
        ref = DatasetRef.from_json(sidecar, registry=self._butler.registry)
        return FileDataset(file_name, ref)

    def ingest(self, datasets: list[FileDataset]) -> None:
        """Ingest a list of FileDataset objects into this Butler.

        Parameters
        ----------
        datasets : `list`
            List of FileDataset to ingest.
        """
        if not datasets:
            return

        dataset_count = len(datasets)
        LOGGER.info(f'starting ingestion of {dataset_count} datasets into butler "{self._butler_config}"')

        remaining_retries = dataset_count + 2
        completed = False
        while not completed:
            try:
                # Attempt ingesting all remaining datasets at once.
                remaining_retries -= 1
                self._butler.ingest(*datasets, transfer="auto")

                LOGGER.info(f"successfully ingested {dataset_count} datasets")
                for dataset in datasets:
                    LOGGER.info(f"ingested file: {dataset.path}")

                completed = True

            except DatasetTypeError:
                # The dataset type of at least one dataset is unknown to the
                # Butler. Register all the dataset types first and try again
                # ingesting the remaining datasets.
                dataset_types: set[DatasetType] = set()
                for dataset in datasets:
                    dataset_types = dataset_types.union({ref.datasetType for ref in dataset.refs})

                for dataset_type in dataset_types:
                    LOGGER.info(f'registering dataset type "{dataset_type.name}"')
                    self._butler.registry.registerDatasetType(dataset_type)

            except MissingCollectionError:
                # The collection for at least one dataset does not exist in
                # the target Butler repo. Create all the necessary collections
                # first and try again ingesting the remaining datasets.
                runs: set[str] = set()
                for dataset in datasets:
                    runs = runs.union({ref.run for ref in dataset.refs})

                for run in runs:
                    LOGGER.info(f'registering run "{run}"')
                    self._butler.registry.registerRun(run)

            except Exception as e:
                # Attempting to ingest a Butler dataset which already exists
                # in the repo raises.
                LOGGER.warning(e)

            if not completed:
                # Not all datasets of this batch were successfully ingested.
                # Identify which datasets are not already in the Butler repo
                # and try ingesting again only those, since ingesting an
                # existing dataset with the same dataset id raises.
                pending_datasets: list[FileDataset] = []
                for dataset in datasets:
                    for dataset_id in {ref.id for ref in dataset.refs}:
                        if self._butler.get_dataset(dataset_id) is None:
                            pending_datasets.append(dataset)
                        else:
                            LOGGER.info(f'file "{dataset.path}" is already ingested')

                if not pending_datasets:
                    LOGGER.info(f"{dataset_count} remaining datasets were ingested")
                    completed = True
                elif remaining_retries == 0:
                    completed = True
                    LOGGER.warning(
                        f"exhausted maximum number of retries but {len(datasets)} out of {dataset_count}"
                        " datasets were not ingested"
                    )
                else:
                    datasets = pending_datasets
                    LOGGER.info(
                        "remaining datasets to ingest after recovering from error:"
                        f" {len(datasets)} out of {dataset_count}"
                    )
