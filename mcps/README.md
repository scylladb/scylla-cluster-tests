# VictoriaLogs Local Setup

## 1. Pull VictoriaLogs Image
```bash
docker pull victoriametrics/victoria-metrics:v1.117.1
```

## 2. Start VictoriaLogs Container
```bash
docker run -d \
  --name victorialogs \
  -p 8428:8428 \
  -v victoria-metrics-data:/victoria-metrics-data \
  victoriametrics/victoria-metrics:v1.117.1 \
  -storageDataPath=victoria-metrics-data \
  -selfScrapeInterval=5s
```

## 3. Open VictoriaLogs UI
Ensure that the service is running by visiting: [http://localhost:8428/](http://localhost:8428/)

## 4. Build an Image for VictoriaLogs MCP Server
```bash
docker build -t mcp-victorialogs:latest .
```

## 5. Start VictoriaLogs MCP Server
Start the MCP server using the \`mcp.json\` configuration file from **VSCode**.
