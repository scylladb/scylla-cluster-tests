FROM openjdk:8 as builder
RUN apt -y update && apt -y install git \
&& git clone --depth 1 --branch add-properties-to-cli-configs https://github.com/KnifeyMoloko/ndbench.git \
&& cd ndbench \
&& ./gradlew build

FROM openjdk:8 as app
COPY --from=builder /root/.gradle /root/.gradle
COPY --from=builder /ndbench /ndbench
