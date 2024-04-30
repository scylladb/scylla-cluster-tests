```
# See latte version at the 'Cargo.toml' file in the 'latte' repo
export LATTE_VERSION=0.25.1-scylladb
export LATTE_DOCKER_IMAGE=scylladb/hydra-loaders:latte-${LATTE_VERSION}
docker build . -t ${LATTE_DOCKER_IMAGE}
docker push ${LATTE_DOCKER_IMAGE}
echo "${LATTE_DOCKER_IMAGE}" > image
```
