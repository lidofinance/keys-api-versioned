FROM golang:1.25-alpine AS building

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/keys-api ./cmd/keys-api \
 && CGO_ENABLED=0 go build -trimpath -o /out/diff ./cmd/diff \
 && CGO_ENABLED=0 go build -trimpath -o /out/bench ./cmd/bench \
 && CGO_ENABLED=0 go build -trimpath -o /out/ingest ./cmd/ingest

FROM alpine:3.21

# wget for the healthcheck, ca-certificates for anything that talks outward.
RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=building /out/ /app/bin/
ENV PATH="/app/bin:${PATH}"

# keys-api is the service. diff checks parity against the canonical service, bench
# measures latency and memory, ingest probes a node without touching a database.
CMD ["keys-api"]
