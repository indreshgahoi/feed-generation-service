# Build context is this directory (services/fanout-worker) -- unlike the
# Go services, this module is self-contained and doesn't need a sibling
# module from elsewhere in the repo.

FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /src
COPY pom.xml .
# Cache dependency downloads separately from source changes.
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package

FROM eclipse-temurin:21-jre-alpine
COPY --from=builder /src/target/fanout-worker-jar-with-dependencies.jar /app/fanout-worker.jar
WORKDIR /app
ENTRYPOINT ["java", "-jar", "fanout-worker.jar"]
