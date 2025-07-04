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

import json
import logging

LOGGER = logging.getLogger(__name__)
RSE_KEY = "dst-rse"
URL_KEY = "dst-url"
RUBIN_BUTLER = "rubin_butler"
RUBIN_SIDECAR = "rubin_sidecar"
SCOPE = "scope"


class Message:
    """Kafka Message representation

    Parameters
    ----------
    kafka_message : `str`
        kafka message
    """

    def __init__(self, kafka_message):
        self._message = kafka_message
        value = self._message.value()
        self.msg = json.loads(value)
        self.payload = self.msg["payload"]

    def get_dst_rse(self) -> str:
        """Getter to retrieve the destination RSE"""
        return self.payload.get(RSE_KEY, None)

    def get_dst_url(self) -> str:
        """Getter to retrieve the destination URL"""
        return self.payload.get(URL_KEY, None)

    def set_dst_url(self, s: str):
        self.payload[URL_KEY] = s

    def get_rubin_butler(self) -> int:
        """Getter to retrieve the flag indicating this is a Butler file"""
        return self.payload.get(RUBIN_BUTLER, None)

    def get_rubin_sidecar(self) -> str:
        """Getter to retrieve the 'sidecar' metadata as a string"""
        return self.payload.get(RUBIN_SIDECAR, None)

    def get_scope(self) -> str:
        """Getter to retrieve the 'scope' metadata as a string"""
        return self.payload.get(SCOPE, None)

    def __str__(self) -> str:
        return f"{self._message}"
