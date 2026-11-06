FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -o /out/raftkv ./cmd/raftkv \
    && CGO_ENABLED=0 go build -trimpath -o /out/raftkvctl ./cmd/raftkvctl

FROM alpine:3.23
RUN addgroup -g 10001 raftkv && adduser -D -u 10001 -G raftkv raftkv \
    && mkdir /data && chown raftkv:raftkv /data
COPY --from=build /out/raftkv /out/raftkvctl /usr/local/bin/
USER raftkv
WORKDIR /app
EXPOSE 8000
ENTRYPOINT ["raftkv"]
