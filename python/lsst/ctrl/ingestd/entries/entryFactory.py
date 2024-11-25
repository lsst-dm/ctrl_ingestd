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

from lsst.ctrl.ingestd.entries.dataType import DataType
from lsst.ctrl.ingestd.entries.entry import Entry
from lsst.ctrl.ingestd.entries.dataProduct import DataProduct
from lsst.ctrl.ingestd.entries.rawFile import RawFile


class EntryFactory:
    """Generic representation of data to put into the Butler
    """
    def __init__(self, rse_butler, mapper):
        self.rse_butler = rse_butler
        self.butler = self.rse_butler.butler
        self.mapper = mapper

    def create_entry(self, message) -> Entry:
        data_type = message.get_rubin_butler()

        # XXX - make this a match-case when we switch to python 3.10
        if data_type == DataType.DATA_PRODUCT:
            return DataProduct(self.butler, message, self.mapper)
        elif data_type == DataType.RAW_FILE:
            return RawFile(self.butler, message, self.mapper)
        else:
            raise ValueError(f"Unknown rubin_butler type: {data_type}")
