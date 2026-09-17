# Build context is the REPO ROOT, not this directory -- the
# protobuf-maven-plugin compiles ../../schemas/proto/cold_tier.proto
# (relative to this module) at `mvn generate-sources` time, so schemas/
# has to be a sibling of services/ inside the image too. Mirrors the same
# pattern the two Go services already use for pkg/sharding.
# Build with: docker build -f services/fanout-worker/Dockerfile .

FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /src

# Cache dependency downloads separately from source/schema changes.
COPY services/fanout-worker/pom.xml ./services/fanout-worker/pom.xml
WORKDIR /src/services/fanout-worker
RUN mvn -q -B dependency:go-offline

WORKDIR /src
COPY schemas ./schemas
COPY services/fanout-worker/src ./services/fanout-worker/src

WORKDIR /src/services/fanout-worker
RUN mvn -q -B package

FROM eclipse-temurin:21-jre-alpine
COPY --from=builder /src/services/fanout-worker/target/fanout-worker-jar-with-dependencies.jar /app/fanout-worker.jar
WORKDIR /app
ENTRYPOINT ["java", "-jar", "fanout-worker.jar"]
