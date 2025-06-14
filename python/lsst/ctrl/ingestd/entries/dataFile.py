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

from lsst.ctrl.ingestd.entries.entry import Entry
from lsst.daf.butler import DatasetRef, FileDataset

LOGGER = logging.getLogger(__name__)


class DataFile(Entry):
    """Entry representing a data product.  Subclasses of this object
    are used to differentiate between data types so ingest can be
    handled differently.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler associated with this entry
    message : `lsst.ctrl.ingestd.message.Message`
        Message representing data to ingest
    mapper : `lsst.ctrl.ingestd.mapper.Mapper`
        Mapping of RSE entry to Butler repo location
    """

    def __init__(self, butler, message, mapper):
        super().__init__(butler, message, mapper)
        self._populate()

    def _populate(self):
        self.data = self._create_file_dataset(self.file_to_ingest, self.sidecar)

    def _create_file_dataset(self, butler_file: str, sidecar: dict) -> FileDataset:
        """Create a FileDatset with sidecar information

        Parameters
        ----------
        butler_file : `str`
            full uri to butler file location
        sidecar : `dict`
            dictionary of the 'sidecar' metadata

        Returns
        -------
        fds :  FileDataset
            FileDataset representing this DataProduct
        """

        ref = DatasetRef.from_json(sidecar, registry=self.butler.registry)
        fds = FileDataset(butler_file, ref)
        return fds

    def get_data(self):
        """Get data associated with this type of object

        Returns
        -------
        data : Any
            data associated with this DataFile
        """
        return self.data

    def __str__(self):
        return f"{self.message}"
