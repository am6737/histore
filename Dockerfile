# Build the manager binary
FROM quay.io/ceph/ceph:v17 as builder
ARG TARGETOS
ARG TARGETARCH=amd64

ARG GOROOT=/usr/local/go
# standard Golang options
ARG GOLANG_VERSION=1.20.4
ARG GO111MODULE=on
ARG GOPROXY=https://goproxy.cn

RUN mkdir -p ${GOROOT} && \
    curl https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-${TARGETARCH}.tar.gz | tar xzf - -C ${GOROOT} --strip-components=1
ENV PATH="${GOROOT}/bin:${GOPATH}/bin:${PATH}"

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY cmd/main.go cmd/main.go
COPY api/ api/
COPY internal/ internal/
COPY pkg/ pkg/

RUN dnf -y install --nodocs \
	librados-devel librbd-devel \
	&& dnf clean all \
	&& rm -rf /var/cache/yum \
    && true

# Build
# the GOARCH has not a default value to allow the binary be built according to the host where the command
# was called. For example, if we call make docker-build in a local env which has the Apple Silicon M1 SO
# the docker BUILDPLATFORM arg will be linux/arm64 when for Apple x86 it will be linux/amd64. Therefore,
# by leaving it empty we can ensure that the container and binary shipped on it will have the same platform.
RUN CGO_ENABLED=1 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o manager cmd/main.go

FROM debian:bullseye-slim
RUN apt update && apt install wget gnupg -y
RUN wget -q -O- 'https://download.ceph.com/keys/release.asc' | apt-key add -
RUN echo deb https://download.ceph.com/debian-pacific/ focal main | tee /etc/apt/sources.list.d/ceph.list
RUN apt-get update && apt-get install -y \
    libcephfs-dev \
    librbd-dev \
    librados-dev
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
COPY --from=builder /workspace/manager .
USER 65532:65532
ENTRYPOINT ["/manager"]