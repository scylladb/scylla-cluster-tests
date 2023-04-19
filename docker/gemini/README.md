```
export GEMINI_VERSION=1.7.8
export GEMINI_DOCKER_IMAGE=scylladb/hydra-loaders:gemini-$GEMINI_VERSION
docker build . -t ${GEMINI_DOCKER_IMAGE} --build-arg version=$GEMINI_VERSION
docker push ${GEMINI_DOCKER_IMAGE}
echo "${GEMINI_DOCKER_IMAGE}" > image
```
