ARG LSST_STACK_VERSION
FROM ghcr.io/lsst/scipipe:${LSST_STACK_VERSION}

WORKDIR /home/lsst

USER root
COPY . ctrl_ingestd/
COPY docker/ingestd/run.sh /home/lsst/run.sh
RUN chown lsst:lsst /home/lsst/run.sh
RUN chown -R lsst:lsst /home/lsst/ctrl_ingestd

USER lsst


RUN git config --global --add safe.directory /home/lsst/ctrl_ingestd && \
    source /opt/lsst/software/stack/loadLSST.bash && \
    setup sconsUtils && \
    setup utils && \
    setup -r ctrl_ingestd && \
    cd ctrl_ingestd && \
    scons


ENTRYPOINT [ "/home/lsst/run.sh" ]
