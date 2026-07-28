FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /workspace
COPY pom.xml ./
RUN mvn --batch-mode dependency:go-offline
COPY src ./src
RUN mvn --batch-mode verify

FROM eclipse-temurin:17-jre-jammy

RUN groupadd --system application \
    && useradd --system --gid application --home-dir /app application
WORKDIR /app
COPY --from=build --chown=application:application /workspace/target/quorum-tickets.jar /app/app.jar

USER application
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
