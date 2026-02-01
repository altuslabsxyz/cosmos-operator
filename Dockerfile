# Build stage
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder

# hadolint ignore=DL3018
RUN apk add --update --no-cache git make

ARG TARGETARCH
ARG TARGETOS

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY *.go .
COPY api/ api/
COPY cmd/ cmd/
COPY controllers/ controllers/
COPY internal/ internal/

ARG VERSION
ARG BUILD_DATE

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build \
    -tags pebbledb \
    -ldflags "-s -w -X github.com/altuslabsxyz/cosmos-operator/internal/version.version=${VERSION}" \
    -a -o manager .

# Build final image from scratch
FROM scratch

LABEL org.opencontainers.image.source=https://github.com/altuslabsxyz/cosmos-operator

WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
