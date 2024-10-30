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

from confluent_kafka import Message as KafkaMessage

LOGGER = logging.getLogger(__name__)


class Message:
    """Kafka Message representation

    Parameters
    ----------
    kafka_message : `str`
        kafka message
    """

    # Dictionary keys of some fields of interest in the payload of a Kafka
    # message.
    RSE_KEY = "dst-rse"
    URL_KEY = "dst-url"
    SCOPE_KEY = "scope"
    NAME_KEY = "name"
    RUBIN_BUTLER_KEY = "rubin_butler"
    RUBIN_SIDECAR_KEY = "rubin_sidecar"

    def __init__(self, kafka_message: KafkaMessage) -> None:
        #
        # The payload of the Kafka message is of the form:
        #
        # {
        #   "event_type": "transfer-done",
        #   "payload": {
        #     ...
        #     "bytes": 98559,
        #     "guid": null,
        #     "previous-request-id": null,
        #     "protocol": "davs",
        #     "scope": "hsc_pdr2_multisite",
        #     "name": "relative/path/to/file",
        #     ...
        #     "rubin_butler": 1,
        #     "rubin_sidecar": "..."
        #   },
        #   "created_at": "2024-10-27 23:02:59.797585"
        # }
        self._message: dict = json.loads(kafka_message.value())
        self._payload: dict = self._message["payload"]

    def __str__(self):
        return json.dumps(self._message)

    @property
    def dst_rse(self) -> str | None:
        """Getter to retrieve the destination RSE"""
        return self._payload.get(self.RSE_KEY, None)

    @property
    def dst_url(self) -> str | None:
        """Getter to retrieve the destination URL"""
        return self._payload.get(self.URL_KEY, None)

    @property
    def rubin_butler(self) -> int | None:
        """Getter to retrieve the flag indicating this is a Butler file"""
        return self._payload.get(self.RUBIN_BUTLER_KEY, None)

    @property
    def rubin_sidecar_str(self) -> str | None:
        """Getter to retrieve the 'sidecar' metadata as a string"""
        if (d := self.rubin_sidecar_dict) is not None:
            return json.dumps(d)
        return None

    @property
    def rubin_sidecar_dict(self) -> dict | None:
        """Getter to retrieve the 'sidecar' metadata as a dict"""
        return self._payload.get(self.RUBIN_SIDECAR_KEY, None)

    @property
    def file_scope(self) -> str | None:
        """Getter to retrieve the 'scope' of the file"""
        return self._payload.get(self.SCOPE_KEY, None)

    @property
    def file_name(self) -> str | None:
        """Getter to retrieve the 'name' of the file"""
        return self._payload.get(self.NAME_KEY, None)
