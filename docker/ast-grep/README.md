```bash
export AST_GREP_DOCKER_IMAGE=scylladb/ast-grep:1.0.0

# Build the image
docker build -t ${AST_GREP_DOCKER_IMAGE} . --push

# Run it against a project directory on your host machine
docker run --rm -v $(pwd):/src -w /src ${AST_GREP_DOCKER_IMAGE}
```
