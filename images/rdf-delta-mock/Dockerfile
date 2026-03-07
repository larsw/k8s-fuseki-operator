FROM alpine:3.21

RUN apk add --no-cache busybox-extras

COPY images/rdf-delta-mock/rdf-delta-server /usr/local/bin/rdf-delta-server

RUN chmod 755 /usr/local/bin/rdf-delta-server

EXPOSE 1066