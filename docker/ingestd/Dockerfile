ARG LSST_STACK_VERSION
FROM lsstsqre/centos:${LSST_STACK_VERSION}

WORKDIR /home/lsst

ARG CTRL_INGESTD_GIT_VERSION
RUN git clone https://github.com/lsst-dm/ctrl_ingestd && \
    cd ctrl_ingestd && \
    git checkout ${CTRL_INGESTD_GIT_VERSION}

#ARG CONFLUENT_KAFKA_VERSION
#RUN source /opt/lsst/software/stack/loadLSST.bash && \
#    mamba install python-confluent-kafka=${CONFLUENT_KAFKA_VERSION}

RUN source /opt/lsst/software/stack/loadLSST.bash && \
    setup sconsUtils && \
    setup utils && \
    setup -r ctrl_ingestd && \
    cd ctrl_ingestd && \
    scons

COPY run.sh /home/lsst/run.sh

ENTRYPOINT [ "/home/lsst/run.sh" ]
