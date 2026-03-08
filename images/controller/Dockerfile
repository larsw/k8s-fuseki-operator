ARG GO_VERSION=1.25.0

FROM golang:${GO_VERSION}-bookworm AS builder

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY api ./api
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
	go build -trimpath \
		-ldflags="-s -w -X fuseki-operator/pkg/version.Version=${VERSION} -X fuseki-operator/pkg/version.Commit=${COMMIT} -X fuseki-operator/pkg/version.Date=${DATE}" \
		-o /workspace/manager ./cmd/manager

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /
COPY --from=builder /workspace/manager /manager

USER nonroot:nonroot

ENTRYPOINT ["/manager"]