FROM golang:1.26.1-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates
ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOWORK=off go build -trimpath -o /out/workflow ./cmd/workflow && \
    CGO_ENABLED=0 GOOS=linux GOWORK=off go build -trimpath -o /out/worker ./cmd/worker

FROM alpine:3.23.3
RUN apk add --no-cache ca-certificates tzdata
COPY --from=build /out/workflow /app/workflow
COPY --from=build /out/worker /app/worker
WORKDIR /app
EXPOSE 8000
VOLUME ["/data/conf"]
USER nobody:nobody
CMD ["/app/workflow", "-conf", "/data/conf"]
