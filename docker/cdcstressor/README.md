```
export CDC_STRESSER_DOCKER_IMAGE=scylladb/hydra-loaders:cdc-stresser-v1
docker build . -t ${CDC_STRESSER_DOCKER_IMAGE}
docker push ${CDC_STRESSER_DOCKER_IMAGE}
echo "${CDC_STRESSER_DOCKER_IMAGE}" > image
```
