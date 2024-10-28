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

import logging

LOGGER = logging.getLogger(__name__)


class Mapper:
    """

    Parameters
    ----------
    rse_dict : `dict`
        rse prefix to physical location prefix dictionary
    """

    def __init__(self, rse_dict: dict):
        self._rse_dict = rse_dict

    def rewrite(self, rse: str, url: str) -> str:
        """Rewrite a Rucio URL for an RSE

        Parameters
        ----------
        rse : `str`
            Rucio RSE name
        url : `str`
            Rucio URL to translate
        """
        mapping_dict = self._rse_dict[rse]
        rucio_prefix = mapping_dict["rucio_prefix"]
        fs_prefix = mapping_dict["fs_prefix"]
        ret = url.replace(rucio_prefix, fs_prefix)
        return ret
