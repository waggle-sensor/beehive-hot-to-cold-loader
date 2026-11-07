FROM golang:1.26.5-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -o /beehive-hot-to-cold-loader .

# Official images supply the pinned CLIs. Both names are on PATH, matching the flag defaults.
FROM influxdb:2.7.7 AS influx
FROM duckdb/duckdb:2.0.0 AS duckdb

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /beehive-hot-to-cold-loader /usr/local/bin/beehive-hot-to-cold-loader
COPY --from=influx /usr/local/bin/influxd /usr/local/bin/influxd
COPY --from=duckdb /duckdb /usr/local/bin/duckdb
EXPOSE 10001
ENTRYPOINT ["beehive-hot-to-cold-loader"]
