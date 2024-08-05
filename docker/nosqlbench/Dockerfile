FROM eclipse-temurin:21-jdk AS build

ARG NOSQLBENCH_GIT_TAG="5.21.2-release"
ARG NOSQLBENCH_GIT_URL="https://github.com/nosqlbench/nosqlbench.git"
ARG SCYLLADB_JAVA_DRIVER="4.18.0.1"

WORKDIR /nosqlbench

RUN apt update \
    && apt install -y git maven xmlstarlet rsync \
    && git clone --depth 1 ${NOSQLBENCH_GIT_URL} . \
    && git fetch --tags \
    && git checkout -b ${NOSQLBENCH_GIT_TAG} tags/${NOSQLBENCH_GIT_TAG}

COPY ./change-to-scylladb-driver.sh .


RUN chmod +x change-to-scylladb-driver.sh \
    && ./change-to-scylladb-driver.sh ${SCYLLADB_JAVA_DRIVER} || exit 1 \
    && mvn \
    -P!build-nb5-appimage \
    -P!build-nbr-appimage \
    -DskipTests=true \
    -Drevision="$NOSQLBENCH_GIT_TAG" \
    package

FROM eclipse-temurin:21-jre-noble AS production

WORKDIR /nosqlbench

RUN apt update && apt upgrade -y

COPY --from=build /nosqlbench/nb5/target/nb5.jar .
COPY ./nosqlbench .

RUN chmod +x nosqlbench

ENV PATH="/nosqlbench:$PATH"

SHELL [ "/bin/bash" ]
