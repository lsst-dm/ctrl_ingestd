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
import math

import yaml

LOGGER = logging.getLogger(__name__)


class Config:
    # Default timeout (seconds) the Kafka client waits for receiving one batch
    # of messages.
    DEFAULT_KAFKA_CLIENT_TIMEOUT: float = 5.0

    # Default number of Kafka messages the client will consume in one batch.
    DEFAULT_KAFKA_NUM_MESSAGES: int = 50

    def __init__(self, filename: str):
        """ingestd configuration

        Parameters
        ----------
        filename : `str`
            Name of the configuration file
        """

        # The configuration file is of the form:
        #
        # kafka_brokers:
        # - broker1.example.org:1234
        # - broker2.example.org:1234
        # - broker3.example.org:1234
        # kafka_topic: "DF_BUTLER_DISK"
        # kafka_num_messages: 50
        # kafka_client_timeout: 1.0
        # rucio_scope: "datastore_dir"
        # butler: "https://host.example.org/path/rse/datastore_dir/butler.yaml"
        #
        #
        # There must be at least one item in 'kafka_brokers'. 'kafka_topic',
        # 'rucio_scope' and 'butler' are all required.
        #
        # Both 'kafka_num_messages' and 'kafka_client_timeout' are optional.
        with open(filename) as file:
            config = yaml.load(file, Loader=yaml.FullLoader)

            if "kafka_brokers" not in config or config["kafka_brokers"] is None:
                raise Exception(f"Can't find 'kafka_brokers' in configuration file {filename}")

            if "kafka_topic" not in config or config["kafka_topic"] is None:
                raise Exception(f"Can't find 'kafka_topic' in configuration file {filename}")

            if "rucio_scope" not in config or config["rucio_scope"] is None:
                raise Exception(f"Can't find 'rucio_scope' in configuration file {filename}")

            if "butler" not in config or config["butler"] is None:
                raise Exception(f"Can't find 'butler' in configuration file {filename}")

            self._brokers: str = ",".join([broker.strip() for broker in config["kafka_brokers"]])
            self._topic: str = config["kafka_topic"].strip()
            self._scope: str = config["rucio_scope"].strip()
            self._butler: str = config["butler"].strip()

            try:
                self._num_messages: int = int(
                    config.get("kafka_num_messages", self.DEFAULT_KAFKA_NUM_MESSAGES)
                )
            except ValueError:
                self._num_messages = self.DEFAULT_KAFKA_NUM_MESSAGES

            try:
                self._timeout: float = float(
                    config.get("kafka_client_timeout", self.DEFAULT_KAFKA_CLIENT_TIMEOUT)
                )
            except ValueError:
                self._timeout = self.DEFAULT_KAFKA_CLIENT_TIMEOUT
            finally:
                if math.isnan(self._timeout):
                    self._timeout = self.DEFAULT_KAFKA_CLIENT_TIMEOUT

            LOGGER.info(f"loaded configuration file: {filename}")
            LOGGER.info(f"kafka brokers: {self._brokers}")
            LOGGER.info(f"listening for messages in topic: {self._topic}")
            LOGGER.info(f"number of kafka messages in one batch: {self._num_messages}")
            LOGGER.info(f"kafka client timeout: {self._timeout:.1f} seconds")
            LOGGER.info(f"rucio scope: {self._scope}")
            LOGGER.info(f"target butler repo: {self._butler}")

    @property
    def brokers(self) -> str:
        """Return a comma-separated list of Kafka brokers to listen to.

        The value returned is of the form:

           broker1.example.org:1234,broker2.example.org:1234,broker3.example.org:1234
        """
        return self._brokers

    @property
    def topic(self) -> str:
        """Return the Kakfa topic to listen to."""
        return self._topic

    @property
    def num_messages(self) -> int:
        """Return the number of Kafka messages to process at a time."""
        return self._num_messages

    @property
    def timeout(self) -> float:
        """Return the length of time to wait for Kafka messages."""
        return self._timeout

    @property
    def scope(self) -> str:
        """Return the Rucio scope."""
        return self._scope

    @property
    def butler(self) -> str:
        """Return the target butler repo alias or configuration file."""
        return self._butler
