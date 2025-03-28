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

            if "topics" not in config:
                raise Exception("Can't find 'topics'")
            self._topic_dict = config["topics"]
            if "brokers" not in config:
                raise Exception("Can't find 'brokers'")
            self._brokers = config["brokers"]
            if "group_id" not in config:
                raise Exception("Can't find 'group_id'")
            self._group_id = config["group_id"]
            self._num_messages = config.get("num_messages", 1)
            self._timeout = config.get("timeout", 1)

            self._butler_config = config["butler"]
            if "repo" not in self._butler_config:
                raise Exception("Can't find 'repo' in 'butler' section")
            self._repo = self._butler_config.get("repo")

            LOGGER.info("butler location: %s", self._repo)
            LOGGER.info("brokers: %s", self._brokers)
            LOGGER.info("rse topics: %s", self._topic_dict.keys())
            LOGGER.info("will batch as many as %d at a time", self._num_messages)

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

    def get_group_id(self) -> str:
        """Getter method for Kafka group_id"""
        return self._group_id

    def get_butler_config(self) -> dict:
        """Getter method for entire configuration dictionary"""
        return self._butler_config

    def get_repo(self) -> str:
        """Getter method for Butler repo location"""
        return self._repo
