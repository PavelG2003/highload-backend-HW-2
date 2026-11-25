# syntax=docker/dockerfile:1
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /workspace

COPY .mvn/ .mvn/
COPY --chmod=0755 mvnw ./
COPY pom.xml ./
COPY src/ src/

RUN --mount=type=cache,target=/root/.m2 ./mvnw --batch-mode --no-transfer-progress package

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /workspace/target/event-pipeline.jar application.jar
USER 10001:10001

# Exec form delivers SIGTERM directly to the Java process.
ENTRYPOINT ["java", "-jar", "/app/application.jar"]
