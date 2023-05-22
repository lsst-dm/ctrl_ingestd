from confluent_kafka import Consumer
import logging
import socket
import os
from typing import Dict
import yaml
from lsst.ctrl.rucio.ingest.config import Config
from lsst.ctrl.rucio.ingest.message import Message
from lsst.ctrl.rucio.ingest.rseButler import RseButler
from lsst.ctrl.rucio.ingest.mapper import Mapper

LOGGER = logging.getLogger(__name__)


class IngestD:

    def __init__(self):
        if "CTRL_RUCIO_INGEST_CONFIG" in os.environ:
            config_file = os.environ["CTRL_RUCIO_INGEST_CONFIG"]
        else:
            raise FileNotFoundError("CTRL_RUCIO_INGEST_CONFIG is not set")
        
        config = Config(config_file)
        rse_dict = config.rses()
        group_id = config.group_id()
        brokers = config.brokers()
        topics = config.topics()
        butler_config = config.butler_config()

        self.mapper = Mapper(rse_dict)

        conf = { 'bootstrap.servers': brokers,
                 'client.id': socket.gethostname,
                 'group.id': group_id,
                 'auto.offset.reset': 'earliest',
                 'enable.auto.commit': True }

        self.consumer = Consumer(conf)
        self.consumer.subscribe(topics)

        self.butler = RseButler(butler_config)

    def run(self):
        while True:
            self.ingest()

    def ingest(self):
        msgs = self.consumer.consume(num_messages=1)

        for msg in msgs:
            message = Message(msg)
            rse, url, has_companion = message.extract_rse_info()
            logging.info(f"{rse=} {url=} {has_companion=}")
            s = self.mapper.rewrite( rse, url)
            print("url: %s, new url: %s" % (url, s))
            print()
            try:
                self.butler.ingest(s)
            except Exception as e:
                print(f'Exception: {e}')

if __name__  == "__main__":
    ingestd = IngestD()
    ingestd.run()
