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

import socket

import yaml
from pydantic import BaseModel, Field, computed_field, model_validator


class _TopicModel(BaseModel):
     rucio_prefix: str
     fs_prefix: str = ""

     @model_validator(mode="after")
     def process_strings(self) -> "_TopicModel":
         if not self.rucio_prefix.endswith("/"):
             self.rucio_prefix += "/"
         if self.fs_prefix and not self.fs_prefix.endswith("/"):
             self.fs_prefix += "/"
         return self

class Config(BaseModel):
    brokers: list[str]
    client_id: str = Field(default_factory=lambda: socket.gethostname())
    group_id: str
    num_messages: int = 50
    timeout: int = 1
    butler_repo: str
    topics: dict[str, _TopicModel] = Field(min_length=1)

    @classmethod
    def load(cls, config_file: str) -> "Config":
        try:
            with open(config_file) as file:
                config_dict = yaml.load(file, Loader=yaml.FullLoader)
            return cls.model_validate(config_dict)
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing {config_file}: {e}") from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error loading {config_file}: {e}") from e


    @computed_field
    def brokers_as_string(self) -> str:
        return ",".join(self.brokers)

    @computed_field
    def topics_as_list(self) -> list:
        return list(self.topics.keys())
