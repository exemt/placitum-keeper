FROM golang:1.25-alpine AS build

WORKDIR /src

ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

ENV CGO_ENABLED=0

ARG VERSION=dev
ARG REVISION=unknown
RUN go build -trimpath -ldflags="-s -w -X main.version=${VERSION} -X main.revision=${REVISION}" -o /out/waf-keeper ./cmd/keeper

FROM alpine:3.22

ARG VERSION=dev
ARG REVISION=unknown

LABEL org.opencontainers.image.title="placitum/keeper" \
      org.opencontainers.image.description="Placitum keeper: source of truth for active datasets (bans, lists, limits)" \
      org.opencontainers.image.source="https://github.com/exemt/placitum-keeper" \
      org.opencontainers.image.licenses="LicenseRef-Placitum" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"

WORKDIR /app

COPY --from=build /out/waf-keeper /usr/local/bin/waf-keeper

RUN adduser -D -H -u 10005 wafkeeper

USER wafkeeper

ENV WAF_KEEPER_HTTP=:8094 \
    WAF_NATS_URL=nats://nats:4222 \
    WAF_KEEPER_DATABASE_URL=postgres://waf:waf@postgres:5432/waf

EXPOSE 8094

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8094/healthz || exit 1

CMD ["waf-keeper"]
