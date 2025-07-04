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
import socket

import lsst.utils.tests
from lsst.ctrl.ingestd.config import Config


class ConfigTestCase(lsst.utils.tests.TestCase):
    def createConfig(self, config_name):
        testdir = os.path.abspath(os.path.dirname(__file__))

        config_file = os.path.join(testdir, "data", config_name)
        self.config = Config.load(config_file)

    def testNoRses(self):
        with self.assertRaises(RuntimeError):
            self.createConfig("notopics.yml")

    def testNoBrokers(self):
        with self.assertRaises(RuntimeError):
            self.createConfig("nobrokers.yml")

    def testNoGroupID(self):
        with self.assertRaises(RuntimeError):
            self.createConfig("nogroupid.yml")

    def testNoRepo(self):
        with self.assertRaises(RuntimeError):
            self.createConfig("norepo.yml")

    def testAttributes(self):
        self.createConfig("ingestd.yml")

        self.assertEqual(self.config.num_messages, 50)
        self.assertEqual(self.config.timeout, 1)

        topic_dict = self.config.topics
        self.assertTrue("XRD1-test1" in topic_dict)
        self.assertTrue("XRD1-test2" in topic_dict)
        self.assertTrue("XRD2-test" in topic_dict)
        self.assertTrue("XRD3-test" in topic_dict)
        self.assertTrue("XRD4-test" in topic_dict)

        self.assertEqual(topic_dict["XRD1-test1"].rucio_prefix, "root://xrd1:1094//rucio/")
        self.assertEqual(topic_dict["XRD1-test1"].fs_prefix, "file:///rucio/disks/xrd1a/rucio/")

        self.assertEqual(topic_dict["XRD1-test2"].rucio_prefix, "root://xrd1:1094//rucio/")
        self.assertEqual(topic_dict["XRD1-test2"].fs_prefix, "file:///rucio/disks/xrd1b/rucio/")

        self.assertEqual(topic_dict["XRD2-test"].rucio_prefix, "root://xrd2:1095//rucio/")
        self.assertEqual(topic_dict["XRD2-test"].fs_prefix, "file:///rucio/disks/xrd2/rucio/")

        self.assertEqual(topic_dict["XRD3-test"].rucio_prefix, "root://xrd3:1096//rucio/test/")
        self.assertEqual(topic_dict["XRD3-test"].fs_prefix, "file:///rucio/disks/xrd3/rucio/")

        self.assertEqual(topic_dict["XRD4-test"].rucio_prefix, "root://xrd4:1097//rucio/test/")
        self.assertEqual(topic_dict["XRD4-test"].fs_prefix, "file:///rucio/disks/xrd4/rucio/")

        topics_list = self.config.topics_as_list
        self.assertTrue("XRD1-test1" in topics_list)
        self.assertTrue("XRD1-test2" in topics_list)
        self.assertTrue("XRD2-test" in topics_list)
        self.assertTrue("XRD3-test" in topics_list)
        self.assertTrue("XRD4-test" in topics_list)

        self.assertEqual(self.config.brokers_as_string, "kafka:9092")
        self.assertEqual(self.config.client_id, socket.gethostname())
        self.assertEqual(self.config.group_id, "my_test_group")

        butler_repo = self.config.butler_repo
        self.assertEqual(butler_repo, "/tmp/repo")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
