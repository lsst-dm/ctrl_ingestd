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
from shutil import copyfile
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

    def __str__(self) -> str:
        return self.val


class RetriesTestCase(lsst.utils.tests.TestCase):
    def createData(self, json_name):
        """Test data product ingest"""

        self.testdir = os.path.abspath(os.path.dirname(__file__))
        json_file = os.path.join(self.testdir, "data", json_name)

        with open(json_file) as f:
            fake_data = f.read()

        fake_msg = FakeKafkaMessage(fake_data)
        self.msg = Message(fake_msg)

        prep_file = os.path.join(self.testdir, "data", "prep.yaml")

        self.repo_dir = tempfile.mkdtemp()
        Butler.makeRepo(self.repo_dir)

        self.butler = RseButler(self.repo_dir)
        instr = Instrument.from_string("lsst.obs.lsst.LsstComCam")

        instr.register(self.butler.butler.registry)
        self.butler.butler.import_(filename=prep_file)

        config_file = os.path.join(self.testdir, "etc", "ingestd.yml")
        config = Config(config_file)
        mapper = Mapper(config.get_topic_dict())

        event_factory = EntryFactory(self.butler, mapper)
        entry = event_factory.create_entry(self.msg)
        return entry


    def testSingleRetry(self):
        entry = self.createData("truncated.json")
        with open("/tmp/data.fits", "w") as f:
            f.write("hi")

        dataset = entry.get_data()
        with self.assertRaises(RuntimeError):
            self.butler._single_ingest(dataset, transfer="auto", retry_as_raw=False)

    def testRetriesBad(self):
        entry = self.createData("truncated2.json")
        with open("/tmp/bad_data.fits", "w") as f:
            f.write("hi")

        self.butler.ingest([entry])

    def testRetries(self):
        entry = self.createData("message330.json")

        tmpdir = "/tmp"
        data_file = "visitSummary_HSC_y_HSC-Y_330_HSC_runs_RC2_w_2023_32_DM-40356_20230814T170253Z.fits"
        fits_file = os.path.join(self.testdir, "data", data_file)
        dest_file = os.path.join(tmpdir, data_file)
        copyfile(fits_file, dest_file)
        
        self.butler.ingest([entry])

    # write clean up
