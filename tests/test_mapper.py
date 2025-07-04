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

import os

import lsst.utils.tests
from lsst.ctrl.ingestd.config import Config
from lsst.ctrl.ingestd.mapper import Mapper


class MapperTestCase(lsst.utils.tests.TestCase):
    def testRewrite(self):
        testdir = os.path.abspath(os.path.dirname(__file__))

        config_file = os.path.join(testdir, "data", "mapper.yml")
        config = Config.load(config_file)

        mapper = Mapper(config.topics)
        s = mapper.rewrite("XRD1-test1", "root://xrd1:1094//rucio/test/28/27/test")
        self.assertEqual(s, "file:///rucio0/test/28/27/test")

        s = mapper.rewrite("XRD1-test2", "root://xrd2:1095//rucio/test/48/47/test")
        self.assertEqual(s, "file:///rucio1/test/48/47/test")

        s = mapper.rewrite("XRD1-test3", "root://xrd3:1096//rucio/test/48/47/test")
        self.assertEqual(s, "file:///rucio2/test/48/47/test")

        s = mapper.rewrite("XRD1-test4", "root://xrd4:1097//rucio/test/48/47/test")
        self.assertEqual(s, "file:///rucio3/test/48/47/test")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
