ARG LSST_STACK_VERSION
FROM lsstsqre/centos:${LSST_STACK_VERSION}

ARG CTRL_RUCIO_INGEST_GIT
RUN git clone https://github.com/lsst-dm/ctrl_rucio_ingest

RUN source /opt/lsst/software/stack/loadLSST.bash && \
    setup sconsUtils && \
    setup utils && \
    setup -r ctrl_rucio_ingest && \
    cd ctrl_rucio_ingest && \
    scons

ARG CONFLUENT_KAFKA_VERSION
RUN source /opt/lsst/software/stack/loadLSST.bash && \
    pip install confluent_kafka==${CONFLUENT_KAFKA_VERSION}
    
ENTRYPOINT [ "python3", "bin/ingestd" ]
