ARG BASE_IMAGE=eclipse-temurin:21-jre-alpine
ARG MAVEN_IMAGE=maven:3.9-eclipse-temurin-21-alpine

FROM ${BASE_IMAGE} AS fuseki-base

ARG JENA_VERSION
ARG JENA_SHA512
ARG ASF_MIRROR=https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=
ARG ASF_ARCHIVE=https://archive.apache.org/dist/

ENV LANG=C.UTF-8 \
    FUSEKI_BASE=/fuseki \
    FUSEKI_HOME=/opt/fuseki

RUN set -eux; \
    test -n "${JENA_VERSION}"; \
    test -n "${JENA_SHA512}"; \
    apk -U upgrade; \
    apk add --no-cache bash curl ca-certificates coreutils findutils gettext tini; \
    mkdir -p /tmp/download

WORKDIR /tmp/download
RUN set -eux; \
    echo "${JENA_SHA512}  fuseki.tar.gz" > fuseki.tar.gz.sha512; \
    (curl --location --silent --show-error --fail --retry 3 --output fuseki.tar.gz "${ASF_MIRROR}jena/binaries/apache-jena-fuseki-${JENA_VERSION}.tar.gz" || \
      curl --location --silent --show-error --fail --retry 3 --output fuseki.tar.gz "${ASF_ARCHIVE}jena/binaries/apache-jena-fuseki-${JENA_VERSION}.tar.gz"); \
    sha512sum -c fuseki.tar.gz.sha512; \
    tar zxf fuseki.tar.gz; \
    mv apache-jena-fuseki-* "${FUSEKI_HOME}"; \
    rm -rf /tmp/download; \
    rm -f "${FUSEKI_HOME}/fuseki.war"; \
    chmod 755 "${FUSEKI_HOME}/fuseki-server"

COPY images/fuseki/shiro.ini ${FUSEKI_HOME}/shiro.ini
COPY images/fuseki/docker-entrypoint.sh /docker-entrypoint.sh
COPY images/fuseki/healthcheck.sh /healthcheck.sh
COPY images/fuseki/load.sh ${FUSEKI_HOME}/load.sh
COPY images/fuseki/tdbloader ${FUSEKI_HOME}/tdbloader
COPY images/fuseki/tdbloader2 ${FUSEKI_HOME}/tdbloader2

RUN set -eux; \
    chmod 755 /docker-entrypoint.sh /healthcheck.sh "${FUSEKI_HOME}/load.sh" "${FUSEKI_HOME}/tdbloader" "${FUSEKI_HOME}/tdbloader2"; \
    addgroup -S fuseki; \
    adduser -S -D -H -G fuseki fuseki; \
    mkdir -p "${FUSEKI_BASE}" /fuseki-extra; \
    chown -R fuseki:fuseki "${FUSEKI_HOME}" "${FUSEKI_BASE}" /fuseki-extra

FROM ${MAVEN_IMAGE} AS launcher-build

ARG JENA_VERSION
ARG ACCUMULO_ACCESS_VERSION=1.0.0-beta
ARG COMMONS_JEXL_VERSION=3.4.0
ARG COMMONS_LOGGING_VERSION=1.3.4
ARG GSON_VERSION=2.11.0

# Install the Fuseki server fat-JAR into the local Maven repository so that it can
# be used as a `provided`-scope compile dependency without pulling from Maven Central.
COPY --from=fuseki-base /opt/fuseki/fuseki-server.jar /tmp/fuseki-server.jar
RUN mvn --batch-mode install:install-file \
    -Dfile=/tmp/fuseki-server.jar \
    -DgroupId=local \
    -DartifactId=fuseki-server \
    "-Dversion=${JENA_VERSION}" \
    -Dpackaging=jar

WORKDIR /build
COPY images/fuseki/pom.xml pom.xml

# Resolve all compile-scope dependencies before copying sources for better layer caching:
# a source-only change will not re-download dependencies.
RUN mvn --batch-mode dependency:resolve \
    "-Djena.version=${JENA_VERSION}" \
    "-Daccumulo.access.version=${ACCUMULO_ACCESS_VERSION}" \
    "-Dcommons.jexl.version=${COMMONS_JEXL_VERSION}" \
    "-Dcommons.logging.version=${COMMONS_LOGGING_VERSION}" \
    "-Dgson.version=${GSON_VERSION}"

COPY images/fuseki/src src

RUN mvn --batch-mode package -DskipTests \
    "-Djena.version=${JENA_VERSION}" \
    "-Daccumulo.access.version=${ACCUMULO_ACCESS_VERSION}" \
    "-Dcommons.jexl.version=${COMMONS_JEXL_VERSION}" \
    "-Dcommons.logging.version=${COMMONS_LOGGING_VERSION}" \
    "-Dgson.version=${GSON_VERSION}"

FROM fuseki-base

COPY --from=launcher-build --chown=fuseki:fuseki /build/target/fuseki-operator-launcher.jar ${FUSEKI_HOME}/fuseki-operator-launcher.jar

LABEL org.opencontainers.image.title="Apache Jena Fuseki"
LABEL org.opencontainers.image.description="Project-owned Apache Jena Fuseki image for fuseki-operator"
LABEL org.opencontainers.image.version="${JENA_VERSION}"

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=10 CMD /healthcheck.sh >/dev/null || exit 1

EXPOSE 3030
VOLUME ["/fuseki"]

USER fuseki
WORKDIR /opt/fuseki

ENTRYPOINT ["/sbin/tini", "--", "/docker-entrypoint.sh"]
CMD ["/opt/fuseki/fuseki-server"]
