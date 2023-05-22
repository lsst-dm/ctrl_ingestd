ARG LSST_STACK_VERSION
FROM lsstsqre/centos:${LSST_STACK_VERSION}

RUN git clone https://github.com/lsst-dm/ctrl_ingestd

ARG CONFLUENT_KAFKA_VERSION
RUN source /opt/lsst/software/stack/loadLSST.bash && \
    pip install confluent_kafka==${CONFLUENT_KAFKA_VERSION} \
    setup sconsUtils && \
    setup utils && \
    setup -r ctrl_ingestd && \
    cd ctrl_ingestd && \
    scons

ENTRYPOINT [ "python3", "bin/ingestd" ]
