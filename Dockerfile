# Base images are pinned by digest: a tag can move under an unchanged commit. Refresh with
# `docker buildx imagetools inspect <image>:<tag>`.
FROM golang:1.26-alpine@sha256:30b1e546a9c7fa3b747c2bdba3a0c7bdf3a02ebb4c5b8405d2f948d7e8aac4ec AS building

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG COMMIT=unknown
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-X main.commit=${COMMIT}" -o /out/keys-api ./cmd/keys-api

FROM building AS building-tools
RUN CGO_ENABLED=0 go build -trimpath -o /out/diff ./cmd/diff \
 && CGO_ENABLED=0 go build -trimpath -o /out/bench ./cmd/bench \
 && CGO_ENABLED=0 go build -trimpath -o /out/ingest ./cmd/ingest

# The stand's measuring tools (docker compose run tools ...), never deployed: diff checks parity
# against the canonical service, bench measures latency and memory, ingest probes a node.
FROM alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf AS tools
RUN apk add --no-cache ca-certificates
COPY --from=building-tools /out/ /app/bin/
ENV PATH="/app/bin:${PATH}"
USER 65532:65532
CMD ["keys-api"]

# The service image, and the default target: no shell, no package manager, one static binary.
# Writes nothing to disk, so any non-root uid and a read-only root filesystem work.
FROM gcr.io/distroless/static-debian13:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329
COPY --from=building /out/keys-api /app/bin/keys-api
ENV PATH="/app/bin:${PATH}"
USER 65532:65532
CMD ["keys-api"]
