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

import lsst.utils.tests
from lsst.ctrl.ingestd.mapper import Mapper


class MapperTestCase(lsst.utils.tests.TestCase):
    def testRewrite(self):
        topic_dict = {
            "XRD1-test1": {
                "rucio_prefix": "root://xrd1:1094//rucio",
                "fs_prefix": "file:///rucio0",
            },
            "XRD1-test2": {
                "rucio_prefix": "root://xrd2:1095//rucio",
                "fs_prefix": "file:///rucio1",
            },
        }
        mapper = Mapper(topic_dict)
        s = mapper.rewrite("XRD1-test1", "root://xrd1:1094//rucio/test/28/27/test")
        self.assertEqual(s, "file:///rucio0/test/28/27/test")
        s = mapper.rewrite("XRD1-test2", "root://xrd2:1095//rucio/test/48/47/test")
        self.assertEqual(s, "file:///rucio1/test/48/47/test")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
