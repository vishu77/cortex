FROM       alpine:3.8
RUN        apk add --no-cache ca-certificates
COPY       fixer /bin/fixer
EXPOSE     80
ENTRYPOINT [ "/bin/fixer" ]

ARG revision
LABEL org.opencontainers.image.title="fixer" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/fixer" \
      org.opencontainers.image.revision="${revision}"
