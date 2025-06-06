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
import socket

import yaml

LOGGER = logging.getLogger(__name__)


class Config:
    def __init__(self, filename: str):
        """ingestd configuration

        Parameters
        ----------
        filename : `str`
            Name of the configuration file
        """
        with open(filename) as file:
            config = yaml.load(file, Loader=yaml.FullLoader)

            self._topic_dict = config.get("topics")
            if not self._topic_dict:
                raise Exception("Can't find 'topics'")

            self._adjust_topic_prefixes()

            brokers = config.get("brokers", None)
            if brokers:
                self._brokers = ",".join(brokers)
            else:
                raise Exception("Can't find 'brokers'")

            self._client_id = config.get("client_id", None)
            if not self._client_id:
                self._client_id = socket.gethostname()

            self._group_id = config.get("group_id", None)
            if not self._group_id:
                raise Exception("Can't find 'group_id'")


            self._num_messages = config.get("num_messages", 50)
            self._timeout = config.get("timeout", 1)

            self._butler_repo = config.get("butler_repo", None)
            if not self._butler_repo:
                raise Exception("Can't find 'butler_repo' in configuration file")

            LOGGER.info("client.id: %s", self._client_id)
            LOGGER.info("group.id: %s", self._group_id)
            LOGGER.info("butler location: %s", self._butler_repo)
            LOGGER.info("brokers: %s", self._brokers)
            LOGGER.info("rse topics: %s", self._topic_dict.keys())
            LOGGER.info("will batch as many as %d at a time", self._num_messages)

    def _adjust_topic_prefixes(self) -> None:
        """adjust default values for rse_prefix and fs_prefix, by adding '/', if needed"""
        topics = self.get_topics()
        for topic in topics:
            mapping_dict = self._topic_dict.get(topic)
            rucio_prefix = mapping_dict.get("rucio_prefix", None)
            if not rucio_prefix:
                raise Exception(f"rucio_prefix not specified in configuration file for topic {topic}")

            if not rucio_prefix.endswith("/"):
                rucio_prefix = rucio_prefix + "/"
                self._topic_dict[topic]["rucio_prefix"] = rucio_prefix

            fs_prefix = mapping_dict.get("fs_prefix", None)
            if fs_prefix:
                if not fs_prefix.endswith("/"):
                    fs_prefix = fs_prefix + "/"
            else:
                fs_prefix = ""
            self._topic_dict[topic]["fs_prefix"] = fs_prefix

    def get_num_messages(self) -> int:
        """Getter method for number of Kafka messages to process at a time"""
        return self._num_messages

    def get_timeout(self) -> int:
        """Getter method for length of time to wait for Kafka messages"""
        return self._timeout

    def get_topic_dict(self) -> dict:
        """Getter method for topic to local prefix mapping"""
        return self._topic_dict

    def get_topics(self) -> list:
        """Getter method for Kafka topics"""
        return list(self._topic_dict.keys())

    def get_brokers(self) -> str:
        """Getter method for Kafka brokers"""
        return self._brokers

    def get_client_id(self) -> str:
        """Getter method for Kafka client.id"""
        return self._client_id

    def get_group_id(self) -> str:
        """Getter method for Kafka group.id"""
        return self._group_id

    def get_butler_repo(self) -> str:
        """Getter method for the butler repo"""
        return self._butler_repo
