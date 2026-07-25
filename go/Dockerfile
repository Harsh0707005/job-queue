FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY leasecmd ./leasecmd
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/go-lease ./leasecmd

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/go-lease /usr/local/bin/go-lease
ENV LISTEN_ADDR=:8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/go-lease"]
