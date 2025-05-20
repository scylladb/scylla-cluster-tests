FROM golang:1.24-alpine AS build

RUN apk add --no-cache git

WORKDIR /src
RUN git clone https://github.com/VictoriaMetrics-Community/mcp-victorialogs.git && \
    cd mcp-victorialogs && \
    CGO_ENABLED=0 GOOS=linux go build -o mcp-victorialogs ./cmd/mcp-victorialogs/main.go


FROM alpine:3.19

COPY --from=build /src/mcp-victorialogs /usr/local/bin/

ENV VL_INSTANCE_ENTRYPOINT="http://localhost:9428"
ENV VL_INSTANCE_BEARER_TOKEN=""
ENV MCP_SERVER_MODE="stdio"

ENTRYPOINT ["mcp-victorialogs"]
