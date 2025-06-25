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
    topic_dict : `dict`
        topic prefix to physical location prefix dictionary
    """

    def __init__(self, topic_dict: dict):
        self._topic_dict = topic_dict

    def rewrite(self, topic: str, url: str) -> str:
        """Rewrite a Rucio URL using topic mapping

        Parameters
        ----------
        topic : `str`
            Kakfa topic name
        url : `str`
            Rucio URL to translate
        """
        topic_entry = self._topic_dict.get(topic, None)
        if not topic_entry:
            raise Exception(f"couldn't find {topic_entry} in topics list")

        rucio_prefix = topic_entry.rucio_prefix
        fs_prefix = topic_entry.fs_prefix

        ret = url.replace(rucio_prefix, fs_prefix)
        return ret
