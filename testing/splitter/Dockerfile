FROM golang:1.26.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY pkg ./pkg
COPY pb ./pb
RUN CGO_ENABLED=0 go build -trimpath -o /out/splitter ./cmd/splitter && \
    CGO_ENABLED=0 go build -trimpath -o /out/splitterctl ./cmd/splitterctl

FROM scratch
COPY --from=build /out/ /usr/local/bin/
WORKDIR /data
EXPOSE 50051
ENTRYPOINT ["/usr/local/bin/splitter"]
