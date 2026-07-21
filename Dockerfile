ARG NIFI_IMAGE=oci.stackable.tech/sandbox/nifi:2.6.0-stackable0.0.0-dev-amd64-with-pr-11002

FROM ${NIFI_IMAGE} AS production

USER root
RUN microdnf install zip
USER stackable

COPY --chown=stackable extensions /tmp/extensions
RUN cd /tmp/extensions \
    && pip install -r bw/requirements.txt -t NAR-INF/bundled-dependencies \
    && zip -r /stackable/nifi/lib/nifi-benz-walter-extensions.nar ./* \
    && rm -rf /tmp/extensions
