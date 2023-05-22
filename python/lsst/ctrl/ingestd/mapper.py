from typing import Dict
import yaml

class Mapper:

    def __init__(self, rse_dict: dict):
        self._rse_dict = rse_dict

    def rewrite(self, rse: str, url: str) -> str:
        mapping_dict = self._rse_dict[rse]
        rucio_prefix = mapping_dict['rucio_prefix']
        fs_prefix = mapping_dict['fs_prefix']
        print(f'{rse=} {url=} --- {rucio_prefix=} {fs_prefix=}')
        ret = url.replace(rucio_prefix, fs_prefix)
        return ret

if __name__ == "__main__":
    rses = {'XRD1': { 'rucio_prefix': 'root://xrd1:1094//rucio', 'fs_prefix': '/rucio'}, 'XRD2': { 'rucio_prefix': 'root://xrd2:1095//rucio', 'fs_prefix': '/rucio'}}
    mapper = Mapper(rses)
    rse = "XRD2"
    url = "root://xrd2:1095//rucio/test/48/47/srp3"
    s = mapper.rewrite(rse, url)
    print(s)
