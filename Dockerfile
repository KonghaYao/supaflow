FROM node:22-alpine AS studio
WORKDIR /src/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
COPY web/go.mod ./web/go.mod
RUN go mod download
COPY . .
COPY --from=studio /src/web/dist ./web/dist
ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/control-plane ./cmd/control-plane
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/worker ./cmd/worker
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/migrate ./cmd/migrate
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/fenix-migrate ./cmd/fenix-migrate
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags='-s -w' -o /out/isolation ./cmd/isolation
RUN mkdir -p /runtime-data/objects

FROM gcr.io/distroless/static-debian12:nonroot AS control-plane
WORKDIR /srv/supaflow
COPY --from=builder /out/control-plane /usr/local/bin/control-plane
COPY migrations/ ./migrations/
COPY --chown=65532:65532 --from=builder /runtime-data/ ./data/
EXPOSE 8080
USER nonroot:nonroot
CMD ["/usr/local/bin/control-plane"]

FROM debian:bookworm-slim AS worker-runtime
ARG DEBIAN_MIRROR=deb.debian.org
RUN sed -i "s|deb.debian.org|${DEBIAN_MIRROR}|g" /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends bubblewrap ca-certificates python3 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/lib/supaflow/isolation \
    && chown 65532:65532 /var/lib/supaflow/isolation
COPY --from=builder /out/worker /usr/local/bin/worker
COPY --from=builder /out/isolation /usr/local/bin/isolation
ENV SUPAFLOW_ISOLATION_BINARY=/usr/local/bin/isolation \
    SUPAFLOW_ISOLATION_CGROUP_ROOT=/sys/fs/cgroup/supaflow \
    SUPAFLOW_ISOLATION_WORKSPACE_ROOT=/var/lib/supaflow/isolation
USER 65532:65532
CMD ["/usr/local/bin/worker"]

FROM gcr.io/distroless/static-debian12:nonroot AS migration
WORKDIR /srv/supaflow
COPY --from=builder /out/migrate /usr/local/bin/migrate
COPY migrations/ ./migrations/
USER nonroot:nonroot
CMD ["/usr/local/bin/migrate"]
