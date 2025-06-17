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
import shutil
import tempfile
from urllib.parse import unquote, urlparse

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
    def testDataProduct(self):
        """Test data product ingest"""

        json_name = "message440.json"
        testdir = os.path.abspath(os.path.dirname(__file__))
        json_file = os.path.join(testdir, "data", json_name)

        with open(json_file) as f:
            fake_data = f.read()

        fake_msg = FakeKafkaMessage(fake_data)
        self.msg = Message(fake_msg)

        prep_file = os.path.join(testdir, "data", "prep.yaml")

        self.repo_dir = tempfile.mkdtemp()
        Butler.makeRepo(self.repo_dir)

        butler = RseButler(self.repo_dir)
        instr = Instrument.from_string("lsst.obs.subaru.HyperSuprimeCam")

        instr.register(butler.butler.registry)
        butler.butler.import_(filename=prep_file)

        config_file = os.path.join(testdir, "etc", "ingestd.yml")
        config = Config.load(config_file)

        mapper = Mapper(config.topics)

        event_factory = EntryFactory(butler, mapper)
        entry = event_factory.create_entry(self.msg)
        fits_file = os.path.join(testdir, "data",
                                 "visitSummary_HSC_y_HSC-Y_330_HSC_runs_RC2_w_2023_32_DM-40356_20230814T170253Z.fits")

        self._copy_tmp_file(fits_file)
        butler.ingest([entry])

    def testRaw(self):
        """Test raw file ingest"""

        json_name = "raw_message.json"
        testdir = os.path.abspath(os.path.dirname(__file__))
        json_file = os.path.join(testdir, "data", json_name)

        with open(json_file) as f:
            fake_data = f.read()

        fake_msg = FakeKafkaMessage(fake_data)
        self.msg = Message(fake_msg)

        self.repo_dir = tempfile.mkdtemp()
        Butler.makeRepo(self.repo_dir)

        butler = RseButler(self.repo_dir)
        instr = Instrument.from_string("lsst.obs.lsst.Latiss")

        instr.register(butler.butler.registry)

        config_file = os.path.join(testdir, "etc", "ingestd.yml")
        config = Config.load(config_file)

        mapper = Mapper(config.topics)

        event_factory = EntryFactory(butler, mapper)
        entry = event_factory.create_entry(self.msg)

        fits_file = os.path.join(testdir, "data", "AT_O_20250113_000004_R00_S00.fits")

        self._copy_tmp_file(fits_file)

        butler.ingest([entry])

    def testDim(self):
        """Test dimension file ingest"""

        json_name = "dim_message.json"
        testdir = os.path.abspath(os.path.dirname(__file__))
        json_file = os.path.join(testdir, "data", json_name)

        with open(json_file) as f:
            fake_data = f.read()

        fake_msg = FakeKafkaMessage(fake_data)
        self.msg = Message(fake_msg)

        self.repo_dir = tempfile.mkdtemp()
        Butler.makeRepo(self.repo_dir)

        butler = RseButler(self.repo_dir)
        instr = Instrument.from_string("lsst.obs.lsst.Latiss")

        instr.register(butler.butler.registry)

        config_file = os.path.join(testdir, "etc", "ingestd.yml")
        config = Config.load(config_file)

        mapper = Mapper(config.topics)

        event_factory = EntryFactory(butler, mapper)
        entry = event_factory.create_entry(self.msg)

        prep_file = os.path.join(testdir, "data", "prep.yaml")

        self._copy_tmp_file(prep_file)
        butler.ingest([entry])

    def testRetry(self):
        """Test data product ingest"""

        json_name = "truncated.json"
        testdir = os.path.abspath(os.path.dirname(__file__))
        json_file = os.path.join(testdir, "data", json_name)

        with open(json_file) as f:
            fake_data = f.read()

        with open("/tmp/data.fits", "w") as f:
            f.write("hi")

        fake_msg = FakeKafkaMessage(fake_data)
        self.msg = Message(fake_msg)

        prep_file = os.path.join(testdir, "data", "prep.yaml")

        self.repo_dir = tempfile.mkdtemp()
        Butler.makeRepo(self.repo_dir)

        butler = RseButler(self.repo_dir)
        instr = Instrument.from_string("lsst.obs.subaru.HyperSuprimeCam")

        instr.register(butler.butler.registry)
        butler.butler.import_(filename=prep_file)

        config_file = os.path.join(testdir, "etc", "ingestd.yml")
        config = Config.load(config_file)

        mapper = Mapper(config.topics)

        event_factory = EntryFactory(butler, mapper)
        entry = event_factory.create_entry(self.msg)
        butler.ingest([entry])

    def _copy_tmp_file(self, prep_file):
        dest_dir = tempfile.mkdtemp()
        src_path = unquote(urlparse(prep_file).path)
        base_name = os.path.basename(src_path)
        dest_path = os.path.join(dest_dir, base_name)

        shutil.copy2(prep_file, dest_path)

class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
