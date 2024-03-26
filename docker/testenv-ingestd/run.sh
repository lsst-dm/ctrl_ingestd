#!/bin/bash
source /opt/lsst/software/stack/loadLSST.bash
setup -r /home/lsst/ctrl_ingestd
/home/lsst/ctrl_ingestd/bin/ingestd
