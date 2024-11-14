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
    def createConfig(self, config_name) -> Config:
        testdir = os.path.abspath(os.path.dirname(__file__))
        config_file = os.path.join(testdir, "data", config_name)
        return Config(config_file)

    def testBrokers(self):
        for file in (
            "config_brokers_absent.yaml",
            "config_brokers_empty.yaml",
        ):
            with self.assertRaises(Exception) as execinfo:
                self.createConfig(file)
            self.assertTrue("'kafka_brokers'" in str(execinfo.exception))

    def testButler(self):
        for file in (
            "config_butler_absent.yaml",
            "config_butler_empty.yaml",
        ):
            with self.assertRaises(Exception) as execinfo:
                self.createConfig(file)
            self.assertTrue("'butler'" in str(execinfo.exception))

    def testTopic(self):
        for file in (
            "config_topic_absent.yaml",
            "config_topic_empty.yaml",
        ):
            with self.assertRaises(Exception) as execinfo:
                self.createConfig(file)
            self.assertTrue("'kafka_topic'" in str(execinfo.exception))

    def testScope(self):
        for file in (
            "config_scope_absent.yaml",
            "config_scope_empty.yaml",
        ):
            with self.assertRaises(Exception) as execinfo:
                self.createConfig(file)
            self.assertTrue("'rucio_scope'" in str(execinfo.exception))

    def testNumMessages(self):
        for file in (
            "config_num_messages_absent.yaml",
            "config_num_messages_invalid.yaml",
            "config_num_messages_nan.yaml",
        ):
            config = self.createConfig(file)
            self.assertEqual(config.num_messages, Config.DEFAULT_KAFKA_NUM_MESSAGES)

    def testTimeout(self):
        for file in (
            "config_timeout_absent.yaml",
            "config_timeout_invalid.yaml",
            "config_timeout_nan.yaml",
        ):
            config = self.createConfig(file)
            self.assertAlmostEqual(config.timeout, Config.DEFAULT_KAFKA_CLIENT_TIMEOUT)

    def testAttributes(self):
        config = self.createConfig("config_gold.yaml")
        self.assertEqual(config.num_messages, 100)
        self.assertAlmostEqual(config.timeout, 5.0)
        self.assertEqual(config.topic, "DF_BUTLER_DISK")

        self.assertEqual(
            config.brokers, "broker1.example.org:1234,broker2.example.org:1234,broker3.example.org:1234"
        )

        self.assertEqual(config.scope, "butler_datastore_dir")

        self.assertEqual(
            config.butler, "https://host.example.org/path/to/rse/butler_datastore_dir/butler.yaml"
        )


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
