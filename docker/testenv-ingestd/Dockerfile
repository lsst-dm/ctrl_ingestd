ARG LSST_STACK_VERSION
FROM ghcr.io/lsst/scipipe:${LSST_STACK_VERSION}

USER root

WORKDIR /home/lsst

USER lsst

COPY run.sh /home/lsst/run.sh

ENTRYPOINT [ "/usr/bin/tail", "-f", "/dev/null" ]
