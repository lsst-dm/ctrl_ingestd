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

from lsst.ctrl.ingestd.entries.dataFile import DataFile

LOGGER = logging.getLogger(__name__)


class DimFile(DataFile):
    """Entry representing a dimension record file to ingest

    Parameters
    ----------
    butler : Butler
        Butler associated with this entry
    message : Message
        Message representing data to ingest
    mapper : Mapper
        Mapping of RSE entry to Butler repo location
    """

    def __init__(self, butler, message, mapper):
        super().__init__(butler, message, mapper)

    def _populate(self):
        self.data = self.file_to_ingest
