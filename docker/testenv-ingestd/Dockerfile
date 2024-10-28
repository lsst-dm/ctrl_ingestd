ARG LSST_STACK_VERSION
FROM lsstsqre/centos:${LSST_STACK_VERSION}

USER root

WORKDIR /home/lsst

USER lsst
ARG CONFLUENT_KAFKA_VERSION
RUN source /opt/lsst/software/stack/loadLSST.bash && \
    pip install confluent_kafka==${CONFLUENT_KAFKA_VERSION}

COPY run.sh /home/lsst/run.sh

ENTRYPOINT [ "/usr/bin/tail", "-f", "/dev/null" ]
