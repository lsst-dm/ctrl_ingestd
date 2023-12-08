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

import lsst.utils.tests
from lsst.ctrl.ingestd.config import Config


class ConfigTestCase(lsst.utils.tests.TestCase):
    def createConfig(self, config_name):
        testdir = os.path.abspath(os.path.dirname(__file__))
        config_file = os.path.join(testdir, "data", config_name)
        self.config = Config(config_file)

    def testNoRses(self):
        with self.assertRaises(Exception) as execinfo:
            self.createConfig("norse.yml")
        self.assertTrue("Can't find 'rses'" in str(execinfo.exception))

    def testNoBrokers(self):
        with self.assertRaises(Exception) as execinfo:
            self.createConfig("nobrokers.yml")
        self.assertTrue("Can't find 'brokers'" in str(execinfo.exception))

    def testNoGroupID(self):
        with self.assertRaises(Exception) as execinfo:
            self.createConfig("nogroupid.yml")
        self.assertTrue("Can't find 'group_id'" in str(execinfo.exception))

    def testNoRepo(self):
        with self.assertRaises(Exception) as execinfo:
            self.createConfig("norepo.yml")
        self.assertTrue("Can't find 'repo' in 'butler' section" in str(execinfo.exception))

    def testNoInstrument(self):
        with self.assertRaises(Exception) as execinfo:
            self.createConfig("noinstrument.yml")
        self.assertTrue("Can't find 'instrument' in 'butler' section" in str(execinfo.exception))

    def testAttributes(self):
        self.createConfig("ingestd.yml")

        self.assertEqual(self.config.get_num_messages(), 50)
        self.assertEqual(self.config.get_timeout(), 1)

        rses = self.config.get_rses()
        self.assertTrue("XRD1" in rses)
        self.assertTrue("XRD2" in rses)
        self.assertTrue("XRD3" in rses)
        self.assertTrue("XRD4" in rses)

        topics = self.config.get_topics()
        self.assertTrue("XRD1" in topics)
        self.assertTrue("XRD2" in topics)
        self.assertTrue("XRD3" in topics)
        self.assertTrue("XRD4" in topics)

        self.assertEqual(self.config.get_brokers(), "kafka:9092")
        self.assertEqual(self.config.get_group_id(), "my_test_group")

        butler_config = self.config.get_butler_config()
        self.assertEqual(butler_config["instrument"], "lsst.obs.subaru.HyperSuprimeCam")
        self.assertEqual(butler_config["repo"], "/tmp/repo")

        self.assertEqual(self.config.get_repo(), "/tmp/repo")
        self.assertEqual(self.config.get_instrument(), "lsst.obs.subaru.HyperSuprimeCam")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
