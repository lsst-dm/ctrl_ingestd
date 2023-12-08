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
from lsst.ctrl.ingestd.message import Message


class FakeKafkaMessage:
    def __init__(self, value):
        self.val = value

    def value(self) -> str:
        return self.val


class MessageTestCase(lsst.utils.tests.TestCase):
    def configure(self, json_name):
        testdir = os.path.abspath(os.path.dirname(__file__))
        json_file = os.path.join(testdir, "data", json_name)

        with open(json_file) as f:
            fake_data = f.read()

        fake_msg = FakeKafkaMessage(fake_data)
        self.msg = Message(fake_msg)

    def testAttributes(self):
        self.configure("message.json")
        self.assertEqual(self.msg.get_dst_rse(), "XRD2")
        self.assertEqual(
            self.msg.get_dst_url(),
            (
                "root://xrd2:1095//rucio/test/srp/data/"
                "calexp_HSC_y_HSC-Y_330_1_54_HSC_runs_RC2_w_2023_32_"
                "DM-40356_20230812T080035Z.fits"
            ),
        )
        self.assertEqual(self.msg.get_rubin_butler(), 1)
        sidecar_str = self.msg.get_rubin_sidecar_str()
        self.assertEqual(
            sidecar_str,
            (
                '{"id": "00a86e99-7661-4f14-ae0d-93d3d4162e26",'
                ' "datasetType": {"name": "calexp", "storageClass": "ExposureF",'
                ' "dimensions": {"names": ["band", "instrument", "detector",'
                ' "physical_filter", "visit"]}}, "dataId": {"dataId":'
                ' {"band": "y", "instrument": "HSC", "detector": 1,'
                ' "physical_filter": "HSC-Y", "visit": 330}}, "run":'
                ' "HSC/runs/RC2/w_2023_32/DM-40356/20230812T080035Z"}'
            ),
        )

    def testNoSidecar(self):
        self.configure("nosidecar.json")
        self.assertIsNone(self.msg.get_rubin_sidecar_dict())
        self.assertIsNone(self.msg.get_rubin_sidecar_str())


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()
