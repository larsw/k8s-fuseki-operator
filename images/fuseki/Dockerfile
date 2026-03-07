ARG BASE_IMAGE=eclipse-temurin:21-jre-alpine
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
    apk add --no-cache bash curl ca-certificates coreutils findutils gettext pwgen tini; \
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

FROM eclipse-temurin:21-jdk-alpine AS launcher-build

COPY --from=fuseki-base /opt/fuseki/fuseki-server.jar /tmp/fuseki-server.jar
COPY images/fuseki/FusekiHttpsLauncher.java /tmp/FusekiHttpsLauncher.java

RUN set -eux; \
    mkdir -p /tmp/classes; \
    javac -cp /tmp/fuseki-server.jar -d /tmp/classes /tmp/FusekiHttpsLauncher.java; \
    jar --create --file /tmp/fuseki-operator-launcher.jar -C /tmp/classes .

FROM fuseki-base

COPY --from=launcher-build --chown=fuseki:fuseki /tmp/fuseki-operator-launcher.jar ${FUSEKI_HOME}/fuseki-operator-launcher.jar

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
