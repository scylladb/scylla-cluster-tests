```
export LATTE_DOCKER_IMAGE=scylladb/hydra-loaders:latte-rust1_73-$(date +'%Y%m%d')
docker build . -t ${LATTE_DOCKER_IMAGE}
docker push ${LATTE_DOCKER_IMAGE}
echo "${LATTE_DOCKER_IMAGE}" > image
```
