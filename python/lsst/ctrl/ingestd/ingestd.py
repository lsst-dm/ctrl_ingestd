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
        self._scope: str = config.scope
        self._butler: RseButler = RseButler(config.butler)

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
            # We may have several instances of ingestd for ingesting into
            # the same butler repo. All those instances must belong to the
            # same Kafka group.
            "group.id": f"{config.topic}-{config.scope}",
        }
        self._consumer: Consumer = Consumer(consumer_config)
        self._consumer.subscribe([config.topic])

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
        entries: list[FileDataset] = []
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

            if message.event_type != "transfer-done":
                # This message is not about a finished transfer. Ignore.
                LOGGER.debug(f"ignoring message with unexpected event type {message.event_type}")
                continue

            if message.file_scope != self._scope:
                # This message is intended for a different butler repo. Ignore.
                LOGGER.debug(f"ignoring file with scope {message.file_scope} which is not configured")
                continue

            if message.rubin_butler != 1:
                LOGGER.warning("shouldn't have gotten this message: %s" % str(message))
                continue

            sidecar = message.rubin_sidecar_dict
            if not sidecar:
                LOGGER.warning("got empty sidecar in Kafka message: %s" % str(message))
                continue

            # Retrieve the name of the file to ingest. Since the RSE uses
            # identity LFN-to-PFN algorithm, the name of the file
            # is relative to the Butler repo's datastore root directory.
            if message.file_name is None:
                LOGGER.warning("failed to retrieve file name from message: %s" % str(message))
                continue

            # Create an object ingestible by the target Butler repo and add
            # it to the list of pending files to ingest.
            try:
                LOGGER.debug(f'creating entry for ingesting file "{message.file_name}"')
                entry = self._butler.create_entry(message.file_name, sidecar)
                entries.append(entry)
            except Exception as e:
                LOGGER.info(e)
                continue

        # If we've got files to ingest, try to ingest them.
        if entries:
            self._butler.ingest(entries)
