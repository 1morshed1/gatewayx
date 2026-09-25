# Multi-stage build for the GatewayX gateway binary.
#
# Stage 1 compiles a static binary (CGO disabled) with the migrations embedded.
# Stage 2 ships it on a minimal Alpine image running as a non-root user.
FROM golang:1.25-alpine AS build
WORKDIR /src

# Cache dependencies first: only re-download when go.mod/go.sum change.
COPY go.mod go.sum ./
RUN go mod download

# Then the source.
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway

FROM alpine:3.20
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
USER app
COPY --from=build /out/gateway /usr/local/bin/gateway

# Public traffic, admin API, metrics.
EXPOSE 8000 8001 9100

# Default command runs the proxy. Use `migrate` / `seed` / `analytics` as the
# container command for those one-shot / sidecar roles.
ENTRYPOINT ["/usr/local/bin/gateway"]
CMD ["serve"]
