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

LOGGER = logging.getLogger(__name__)


class Entry:
    """Generic representation of data to put into the Butler

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
        self.butler = butler
        self.message = message
        self.mapper = mapper

        self.data_type = message.get_rubin_butler()
        self.sidecar = message.get_rubin_sidecar()
        LOGGER.debug(f"{message=} {self.data_type=} {self.sidecar=}")
        if self.data_type is None:
            raise RuntimeError(f"data_type not specified in: {message}")

        # Rewrite the Rucio URL to actual file location
        dst_rse = self.message.get_dst_rse()
        scope = self.message.get_scope()
        dst_url = self.message.get_dst_url()

        topic = f"{dst_rse}-{scope}"
        self.file_to_ingest = self.mapper.rewrite(topic, dst_url)

        if self.file_to_ingest == dst_url:
            # Avoid E501
            LOGGER.warning(f"attempt to map {self.file_to_ingest} to same file")

    def get_data_type(self):
        return self.data_type

    def get_data(self):
        raise RuntimeError("Shouldn't call Entry.get_data directly")

    def __str__(self):
        return self.get_data()
