FROM openjdk:8 as builder
RUN apt -y update && apt -y install git && \
    git clone --branch v0.4.14 https://github.com/Netflix/ndbench.git && \
    cd ndbench          && \
    ./gradlew build

FROM openjdk:8 as app
COPY --from=builder /root/.gradle /root/.gradle
COPY --from=builder /ndbench /ndbench
