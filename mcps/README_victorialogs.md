# VictoriaLogs Local Setup

## 1. Pull VictoriaLogs Image
```bash
docker pull docker.io/victoriametrics/victoria-logs:v1.22.2-victorialogs
```

## 2. Start VictoriaLogs Container
```bash
docker run -d \
  -p 9428:9428 \
  -v ./victoria-logs-data:/victoria-logs-data \
  docker.io/victoriametrics/victoria-logs:v1.22.2-victorialogs \
  -storageDataPath=victoria-logs-data
```

## 3. Open VictoriaLogs UI
Ensure that the service is running by visiting: [http://localhost:9428/](http://localhost:9428/)

## 4. Build an Image for VictoriaLogs MCP Server
```bash
docker build -t mcp-victorialogs:latest .
```

## 5. Start VictoriaLogs MCP Server
Start the MCP server using the \`mcp.json\` configuration file from **VSCode**.
