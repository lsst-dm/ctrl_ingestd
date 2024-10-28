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
import uuid

from confluent_kafka import Consumer
from lsst.ctrl.ingestd.config import Config
from lsst.ctrl.ingestd.message import Message
from lsst.ctrl.ingestd.rseButler import RseButler
from lsst.daf.butler import FileDataset

LOGGER = logging.getLogger(__name__)

class IngestD:
    """Entry point for ingestd"""

    def __init__(self, config_file: str) -> None:
        # Load configuration
        config = Config(config_file)
        self._num_messages = config.get_num_messages()
        self._timeout = config.get_timeout()

        # Prepare the Kafka consumer. The documentation of the consumer
        # is available online at
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html
        consumer_config = {
            "bootstrap.servers": config.get_brokers(),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            # Use this hostname suffixed by a unique identifier as this
            # Kafka client id, so that we can run several ingestd instances
            # on the same host.
            "client.id": f"{socket.gethostname}-{str(uuid.uuid4()).encode('utf-8')}",
            # All instances of ingestd listening to the same topic will belong
            # to the same consumer group. Since the topic is typically the
            # name of the destination RSE, it is possible to have several
            # ingestd instances processing messages for a single RSE.
            "group.id": config.get_topic(),
        }
        self._consumer = Consumer(consumer_config)
        self._consumer.subscribe([config.get_topic()])

        # Create a RseButler object for each Butler repo in the configuration.
        # The key of the dictionary is the Butler alias and the value is
        # a RseButler object.
        #
        # Note that the butler alias is identical to the Rucio scope of the
        # files we are going to process, so we can use that scope to retrieve
        # the target Butler repo to ingest the file into.
        self._butlers: dict[str, RseButler] = {}
        for butler_alias, butler_configuration in config.get_butlers().items():
            self._butlers[butler_alias] = RseButler(butler_configuration)

    def run(self):
        """continually process messages"""
        while True:
            self._process()

    def _process(self):
        """process one set of messages"""

        # read up to self.num_messages, with a timeout of self.timeout
        msgs = self._consumer.consume(num_messages=self._num_messages, timeout=self._timeout)
        # just return if there are no messages
        if msgs is None:
            return

        # cycle through all the messages, rewriting the Rucio URL
        # so the files can be directly ingested in their actual location,
        # and put the into a list
        entries: dict[str, list[FileDataset]] = {key: [] for key in self._butlers.keys()}
        for msg in msgs:
            try:
                message = Message(msg)
            except Exception as e:
                logging.info(msg.value())
                logging.info(e)
                continue

            rubin_butler = message.get_rubin_butler()
            sidecar = message.get_rubin_sidecar_dict()
            logging.info(f"{message=} {rubin_butler=} {sidecar=}")

            # TODO: check values of rubin_butler (must be 1) and rubin_sidecar
            # must be a non-empty str.

            if rubin_butler is None:
                logging.warning("shouldn't have gotten this message: %s" % message)
                continue

            # Retrieve the name of the file to ingest. Since the RSE uses
            # identity LFN-to-PFN algorithm, the name of the file
            # is relative to the Butler repo's datastore root directory.
            file_to_ingest = message.get_file_name()
            if file_to_ingest is None:
                logging.warning(f"failed to retrieve file name from message {message}")
                continue

            # Retrieve the target Butler repo this file is intended for
            # ingestion.
            scope = message.get_file_scope()
            if scope not in self._butlers.keys():
                # We got a message for a target butler repo which is not
                # configured.
                url = message.get_dst_url()
                logging.warning(
                    f'ignoring file "{url}" targeted for ingestion into butler'
                    f' "{scope}" which is not configured'
                )
                continue

            # create an object that's ingestible by the target butler repo
            # and add it to the list of pending files to ingest
            try:
                # Ensure the file name is relative to the Butler repo.
                file_to_ingest = file_to_ingest.lstrip("/")
                logging.info(f'creating entry for file "{file_to_ingest}" into butler "{scope}"')
                entry = self._butlers[scope].create_entry(file_to_ingest, sidecar)
                entries[scope].append(entry)
            except Exception as e:
                logging.info(e)
                continue

        # if we've got anything in the list, try and ingest it.
        for scope in entries.keys():
            if len(entries[scope]) > 0:
                self._butlers[scope].ingest(entries[scope])
