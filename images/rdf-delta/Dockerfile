ARG BASE_IMAGE=eclipse-temurin:21-jdk-alpine
FROM ${BASE_IMAGE} AS builder

ARG JENA_VERSION
ARG JENA_COMMANDS_SHA512
ARG ASF_MIRROR=https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=
ARG ASF_ARCHIVE=https://archive.apache.org/dist/

RUN set -eux; \
    test -n "${JENA_VERSION}"; \
    test -n "${JENA_COMMANDS_SHA512}"; \
    apk -U upgrade; \
    apk add --no-cache curl ca-certificates tar

WORKDIR /tmp/download
RUN set -eux; \
    echo "${JENA_COMMANDS_SHA512}  jena.tar.gz" > jena.tar.gz.sha512; \
    (curl --location --silent --show-error --fail --retry 3 --output jena.tar.gz "${ASF_MIRROR}jena/binaries/apache-jena-${JENA_VERSION}.tar.gz" || \
      curl --location --silent --show-error --fail --retry 3 --output jena.tar.gz "${ASF_ARCHIVE}jena/binaries/apache-jena-${JENA_VERSION}.tar.gz"); \
    sha512sum -c jena.tar.gz.sha512; \
    tar zxf jena.tar.gz; \
    mv apache-jena-* /opt/rdf-delta

COPY images/rdf-delta/RDFDeltaServer.java /tmp/RDFDeltaServer.java

RUN set -eux; \
    mkdir -p /tmp/classes; \
    javac -cp '/opt/rdf-delta/lib/*' -d /tmp/classes /tmp/RDFDeltaServer.java; \
    jar --create --file /tmp/rdf-delta-server.jar -C /tmp/classes .

FROM eclipse-temurin:21-jre-alpine

ARG JENA_VERSION

RUN set -eux; \
    apk -U upgrade; \
    apk add --no-cache ca-certificates

COPY --from=builder /opt/rdf-delta /opt/rdf-delta
COPY --from=builder /tmp/rdf-delta-server.jar /opt/rdf-delta/rdf-delta-server.jar
COPY images/rdf-delta/rdf-delta-server /usr/local/bin/rdf-delta-server

RUN set -eux; \
    chmod 755 /usr/local/bin/rdf-delta-server; \
    mkdir -p /var/lib/rdf-delta

LABEL org.opencontainers.image.title="Apache Jena RDF Delta Runtime"
LABEL org.opencontainers.image.description="Project-owned Jena-backed RDF Delta image for fuseki-operator"
LABEL org.opencontainers.image.version="${JENA_VERSION}"

EXPOSE 1066
VOLUME ["/var/lib/rdf-delta"]

CMD ["rdf-delta-server"]