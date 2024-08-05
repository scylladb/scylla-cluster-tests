FROM eclipse-temurin:21-jdk AS build

ARG GIT_TAG="5.21.2-release"
ARG GIT_URL="https://github.com/nosqlbench/nosqlbench.git"
ARG SCYLLADB_JAVA_DRIVER="4.18.0.1"

WORKDIR /nosqlbench

RUN apt update \
    && apt install -y git maven xmlstarlet rsync \
    && git clone --depth 1 ${GIT_URL} . \
    && git fetch --tags \
    && git checkout -b ${GIT_TAG} tags/${GIT_TAG}

COPY ./change-to-scylladb-driver.sh .


# There are many profiles that can be turned off
# https://github.com/nosqlbench/nosqlbench/blob/5.21.2-release/nb-adapters/pom.xml#L56

RUN chmod +x change-to-scylladb-driver.sh \
    && ./change-to-scylladb-driver.sh $SCYLLADB_JAVA_DRIVER \
    && mvn \
    -P!build-nb5-appimage \
    -P!build-nbr-appimage \
    # -P!adapter-milvus-module \
    # -P!adapter-mongodb-module \
    # -P!adapter-pulsar-module \
    # -P!adapter-dataapi-module \
    # -P!adapter-qdrant-module \
    # -P!adapter-amqp-module \
    # -P!adapter-neo4j-module \
    -DskipTests=true \
    -Drevision="$GIT_TAG" \
    package

FROM eclipse-temurin:21-jre-noble AS production

WORKDIR /nosqlbench

RUN apt update && apt upgrade -y

COPY --from=build /nosqlbench/nb5/target/nb5.jar .
COPY ./nosqlbench .

RUN chmod +x nosqlbench

ENV PATH="/nosqlbench:$PATH"

SHELL [ "/bin/bash" ]

CMD ["nosqlbench"]