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

import os.path
import tempfile

import lsst.utils.tests
from lsst.ctrl.ingestd.config import Config
from lsst.ctrl.ingestd.entries.entryFactory import EntryFactory
from lsst.ctrl.ingestd.mapper import Mapper
from lsst.ctrl.ingestd.message import Message
from lsst.ctrl.ingestd.rseButler import RseButler
from lsst.daf.butler import Butler
from lsst.pipe.base import Instrument


class FakeKafkaMessage:
    def __init__(self, value):
        self.val = value

    def value(self) -> str:
        return self.val


class RseButlerTestCase(lsst.utils.tests.TestCase):
    def testRseButler(self):
        json_name = "message.json"
        testdir = os.path.abspath(os.path.dirname(__file__))
        json_file = os.path.join(testdir, "data", json_name)

        with open(json_file) as f:
            fake_data = f.read()

        fake_msg = FakeKafkaMessage(fake_data)
        self.msg = Message(fake_msg)

        testdir = os.path.abspath(os.path.dirname(__file__))
        prep_file = os.path.join(testdir, "data", "prep.yaml")

        self.repo_dir = tempfile.mkdtemp()
        Butler.makeRepo(self.repo_dir)

        butler = RseButler(self.repo_dir)
        instr = Instrument.from_string("lsst.obs.subaru.HyperSuprimeCam")

        instr.register(butler.butler.registry)
        butler.butler.import_(filename=prep_file)

        self.temp_file = tempfile.NamedTemporaryFile()

        config_file = os.path.join(testdir, "etc", "ingestd.yml")
        config = Config(config_file)
        mapper = Mapper(config.get_rses())

        event_factory = EntryFactory(butler, mapper)
        entry = event_factory.create_entry(self.msg)
        butler.ingest([entry])


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
