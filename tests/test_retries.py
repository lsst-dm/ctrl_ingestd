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


class RetriesTestCase(lsst.utils.tests.TestCase):
    def setUp(self):
        self.test_dir = os.path.abspath(os.path.dirname(__file__))
        self.repo_dir = tempfile.mkdtemp()
        self.bad_dir = tempfile.mkdtemp()
        self.single_dir = tempfile.mkdtemp()
        self.tmp_multi_dir = tempfile.mkdtemp()
        self.tmp_retry_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.repo_dir, ignore_errors=True)
        shutil.rmtree(self.bad_dir, ignore_errors=True)
        shutil.rmtree(self.single_dir, ignore_errors=True)
        shutil.rmtree(self.tmp_multi_dir, ignore_errors=True)
        shutil.rmtree(self.tmp_retry_dir, ignore_errors=True)

    def createRseButler(self):
        prep_file = os.path.join(self.test_dir, "data", "prep.yaml")

        Butler.makeRepo(self.repo_dir)

        rse_butler = RseButler(self.repo_dir)
        instr = Instrument.from_string("lsst.obs.lsst.LsstComCam")

        instr.register(rse_butler.butler.registry)
        rse_butler.butler.import_(filename=prep_file)
        return rse_butler

    def createData(self, butler, json_name, dest_path=None):
        """Test data product ingest"""

        json_file = os.path.join(self.test_dir, "data", json_name)

        with open(json_file) as f:
            fake_data = f.read()

        fake_msg = FakeKafkaMessage(fake_data)
        self.msg = Message(fake_msg)
        if dest_path:
            self.msg.set_dst_url(dest_path)

        config_file = os.path.join(self.test_dir, "etc", "ingestd.yml")

        config = Config.load(config_file)
        mapper = Mapper(config.topics)

        event_factory = EntryFactory(butler, mapper)
        entry = event_factory.create_entry(self.msg)
        return entry

    def createMultiTestEnv(self):
        rse_butler = self.createRseButler()
        instr = Instrument.from_string("lsst.obs.subaru.HyperSuprimeCam")
        instr.register(rse_butler.butler.registry)

        data_file = "visitSummary_HSC_y_HSC-Y_330_HSC_runs_RC2_w_2023_32_DM-40356_20230814T170253Z.fits"
        fits_file = os.path.join(self.test_dir, "data", data_file)
        dest_path = self._copy_tmp_file(fits_file, self.tmp_multi_dir)

        good_entry = self.createData(rse_butler, "message440.json", dest_path)

        bad_path = f"{self.tmp_multi_dir}/bad_data.fits"
        with open(bad_path, "w") as f:
            f.write("hi")

        bad_entry = self.createData(rse_butler, "truncated2.json", bad_path)
        return rse_butler, good_entry, bad_entry

    def testSingle(self):
        """Test the single ingest method"""

        rse_butler = self.createRseButler()

        data_path = f"{self.single_dir}/data.fits"
        with open(data_path, "w") as f:
            f.write("hi")

        entry = self.createData(rse_butler, "truncated.json", data_path)

        dataset = entry.get_data()
        with self.assertRaises(RuntimeError) as context:
            rse_butler._single_ingest(dataset, transfer="auto", retry_as_raw=False)
        self.assertEqual(str(context.exception), f"couldn't ingest {data_path}")

    def testBadFile(self):
        """Test a bad file ingest"""

        rse_butler = self.createRseButler()
        bad_path = f"{self.bad_dir}/bad_data2.fits"
        with open(bad_path, "w") as f:
            f.write("hi")

        entry = self.createData(rse_butler, "truncated2.json", bad_path)

        rse_butler.ingest([entry])

    def testRetries(self):
        """Test ingest interface"""

        rse_butler = self.createRseButler()

        data_file = "visitSummary_HSC_y_HSC-Y_330_HSC_runs_RC2_w_2023_32_DM-40356_20230814T170253Z.fits"
        fits_file = os.path.join(self.test_dir, "data", data_file)
        dest_path = self._copy_tmp_file(fits_file, self.tmp_retry_dir)

        entry = self.createData(rse_butler, "message330.json", dest_path)
        rse_butler.ingest([entry])

    def testBadGood(self):
        """Test ingest bad file, then good file"""

        rse_butler, good_entry, bad_entry = self.createMultiTestEnv()
        rse_butler.ingest([bad_entry, good_entry])

    def testGoodBad(self):
        """Test ingest good file file, then bad file"""

        rse_butler, good_entry, bad_entry = self.createMultiTestEnv()
        rse_butler.ingest([good_entry, bad_entry])

    def testMultiSingleIngest(self):
        """Test ingest good file, then re-ingest of good file"""

        rse_butler, good_entry, bad_entry = self.createMultiTestEnv()
        rse_butler._single_ingest(good_entry.get_data(), transfer="auto", retry_as_raw=False)
        with self.assertRaises(RuntimeError):
            rse_butler._single_ingest(good_entry.get_data(), transfer="auto", retry_as_raw=False)

    def _copy_tmp_file(self, prep_file, dest_dir):
        src_path = unquote(urlparse(prep_file).path)
        base_name = os.path.basename(src_path)
        dest_path = os.path.join(dest_dir, base_name)

        shutil.copy2(prep_file, dest_path)
        return f"file://{dest_path}"
