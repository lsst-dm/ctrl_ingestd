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
import os
import socket

from confluent_kafka import Consumer
from lsst.ctrl.ingestd.config import Config
from lsst.ctrl.ingestd.entries.entryFactory import EntryFactory
from lsst.ctrl.ingestd.mapper import Mapper
from lsst.ctrl.ingestd.message import Message
from lsst.ctrl.ingestd.rseButler import RseButler

LOGGER = logging.getLogger(__name__)

CTRL_INGESTD_CONFIG = "CTRL_INGESTD_CONFIG"


class IngestD:
    """Entry point for ingestd"""

    def __init__(self):
        if CTRL_INGESTD_CONFIG in os.environ:
            self.config_file = os.environ[CTRL_INGESTD_CONFIG]
        else:
            raise FileNotFoundError("CTRL_INGESTD_CONFIG is not set")

        config = Config(self.config_file)
        rse_dict = config.get_rses()
        group_id = config.get_group_id()
        brokers = config.get_brokers()
        topics = config.get_topics()

        self.num_messages = config.get_num_messages()
        self.timeout = config.get_timeout()

        self.mapper = Mapper(rse_dict)

        conf = {
            "bootstrap.servers": brokers,
            "client.id": socket.gethostname,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }

        self.consumer = Consumer(conf)
        self.consumer.subscribe(topics)

        self.rse_butler = RseButler(config.get_repo())
        self.entry_factory = EntryFactory(self.rse_butler, self.mapper)

    def run(self):
        """continually process messages"""
        while True:
            self.process()

    def process(self):
        """process one set of messages"""

        # read up to self.num_messages, with a timeout of self.timeout
        msgs = self.consumer.consume(num_messages=self.num_messages, timeout=self.timeout)
        # just return if there are no messages
        if msgs is None:
            return

        # cycle through all the messages, rewriting the Rucio URL
        # so the files can be directly ingested in their actual location,
        # and put the into a list
        entries = []
        for msg in msgs:
            try:
                message = Message(msg)
            except Exception as e:
                logging.info(msg.value())
                logging.info(e)
                continue
            entry = self.entry_factory.create_entry(message)
            entries.append(entry)
        # if we've got anything in the list, try and ingest it.
        if len(entries) > 0:
            self.rse_butler.ingest(entries)


if __name__ == "__main__":
    ingestd = IngestD()
    ingestd.run()
