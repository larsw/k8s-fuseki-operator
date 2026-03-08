FROM scratch

LABEL operators.operatorframework.io.bundle.mediatype.v1=registry+v1 \
      operators.operatorframework.io.bundle.manifests.v1=manifests/ \
      operators.operatorframework.io.bundle.metadata.v1=metadata/

COPY bundle/manifests /manifests/
COPY bundle/metadata /metadata/