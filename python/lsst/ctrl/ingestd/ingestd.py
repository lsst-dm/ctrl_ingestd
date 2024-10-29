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
        self._num_messages: int = config.num_messages
        self._timeout: float = config.timeout

        # Prepare the Kafka consumer. The documentation of the consumer
        # is available online at
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html
        consumer_config = {
            "bootstrap.servers": config.brokers,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            # Use this hostname suffixed by a unique identifier as this
            # Kafka client id, so that we can run several ingestd instances
            # on the same host.
            "client.id": f"{socket.gethostname()}-{str(uuid.uuid4())}",
            # All instances of ingestd listening to the same topic will belong
            # to the same consumer group. Since the topic is typically the
            # name of the destination RSE, it is possible to have several
            # ingestd instances processing messages for a single RSE.
            "group.id": config.topic,
        }
        self._consumer: Consumer = Consumer(consumer_config)
        self._consumer.subscribe([config.topic])

        # Create a RseButler object for each Butler repo in the configuration.
        # The key of the dictionary is the Butler alias and the value is
        # a RseButler object.
        #
        # Note that the butler alias is identical to the Rucio scope of the
        # files we are going to process, so we can use that scope to retrieve
        # the target Butler repo to ingest the file into.
        self._butlers: dict[str, RseButler] = {}
        for scope, butler_config in config.butlers.items():
            self._butlers[scope] = RseButler(butler_config)

    def run(self) -> None:
        """Continually process messages."""
        while True:
            self._process()

    def _process(self) -> None:
        """Process one set of messages."""

        # Read up to self._num_messages, with a timeout of self._timeout.
        # Return immediately if there are no messages to process.
        msgs = self._consumer.consume(num_messages=self._num_messages, timeout=self._timeout)
        if msgs is None:
            return

        # Cycle through all the messages, create a Butler Dataset for each
        # file to ingest and ingest them into the target Butler repo.
        entries: dict[str, list[FileDataset]] = {scope: [] for scope in self._butlers}
        for msg in msgs:
            if msg.error() is not None:
                LOGGER.warning("could not retrieve Kafka message: %s" % msg.error().str())
                continue

            try:
                message = Message(msg)
            except Exception as e:
                LOGGER.info("error processing message: %s" % msg.value())
                LOGGER.info(e)
                continue

            LOGGER.debug(f"processing message {str(message)}")

            rubin_butler = message.rubin_butler
            if rubin_butler is None or rubin_butler != 1:
                LOGGER.warning("shouldn't have gotten this message: %s" % str(message))
                continue

            sidecar = message.rubin_sidecar_dict
            if not sidecar:
                LOGGER.warning("got empty sidecar in Kafka message: %s" % str(message))

            # Retrieve the name of the file to ingest. Since the RSE uses
            # identity LFN-to-PFN algorithm, the name of the file
            # is relative to the Butler repo's datastore root directory.
            file_to_ingest = message.file_name
            if file_to_ingest is None:
                LOGGER.warning("failed to retrieve file name from message: %s" % str(message))
                continue

            # Retrieve the target Butler repo this file is intended for
            # ingestion into.
            scope = message.file_scope
            if scope not in self._butlers:
                # We got a message for a target butler repo which is not
                # configured.
                url = message.dst_url
                LOGGER.warning(
                    f'ignoring file "{url}" targeted for ingestion into butler'
                    f' "{scope}" which is not configured'
                )
                continue

            # Create an object that's ingestible by the target Butler repo
            # and add it to the list of pending files to ingest
            try:
                LOGGER.debug(f'creating entry for ingesting file "{file_to_ingest}" into butler "{scope}"')
                entry = self._butlers[scope].create_entry(file_to_ingest, sidecar)
                entries[scope].append(entry)
            except Exception as e:
                LOGGER.info(e)
                continue

        # If we've got files to ingest in the list, try and ingest them.
        for scope in entries:
            if len(entries[scope]) > 0:
                self._butlers[scope].ingest(entries[scope])
