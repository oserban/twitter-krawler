FROM maven:3-jdk-14 as builder
LABEL maintainer="ovidiu@roboslang.org"

WORKDIR /code

COPY src/ src/
COPY pom.xml .

RUN rm -rf target/ && mvn clean compile package

# The optimized docker image
FROM azul/zulu-openjdk-alpine:15-jre

RUN apk add --no-cache bash

ENV APPLICATION_PATH /app
ENV APPLICATION_USER twitter

RUN adduser -D -g '' $APPLICATION_USER && mkdir $APPLICATION_PATH && chown -R $APPLICATION_USER $APPLICATION_PATH
USER $APPLICATION_USER
WORKDIR $APPLICATION_PATH

COPY --from=builder /code/target/twitter-krawler-*-jar-with-dependencies.jar .
COPY scripts/start.sh .
COPY scripts/include.sh .

CMD bash start.sh