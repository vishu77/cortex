FROM       alpine:3.8
RUN        apk add --no-cache ca-certificates
COPY       overrides-exporter /
ENTRYPOINT ["/overrides-exporter"]

ARG revision
LABEL org.opencontainers.image.title="overrides-exporter" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/cmd/overrides-exporter" \
      org.opencontainers.image.revision="${revision}"
