from typing import Dict
import yaml

class Config:

    def __init__(self, filename):
        with open(filename) as file:
            config = yaml.load(file, Loader=yaml.FullLoader)

            if 'rses' not in config:
                raise Exception("Can't find 'rses'")
            self._rse_dict = config['rses']
            if 'brokers' not in config:
                raise Exception("Can't find 'brokers'")
            self._brokers = config['brokers']
            if 'group_id' not in config:
                raise Exception("Can't find 'group_id'")
            self._group_id = config['group_id']
            self._butler_config = config['butler']

    def rewrite(self, rse: str, url: str) -> str:
        mapping_dict = self._rse_dict[rse]
        rucio_prefix = mapping_dict['rucio_prefix']
        fs_prefix = mapping_dict['fs_prefix']
        print('reswrite: %s %s <> %s %s' % (rse, url, rucio_prefix, fs_prefix))
        ret = url.replace(rucio_prefix, fs_prefix)
        return ret

    def rses(self) -> dict:
        return self._rse_dict

    def topics(self) -> list:
        return list(self._rse_dict.keys())

    def brokers(self) -> str:
        return self._brokers

    def group_id(self) -> str:
        return self._group_id

    def butler_config(self) -> dict:
        return self._butler_config
