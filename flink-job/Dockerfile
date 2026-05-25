FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /build
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package

FROM flink:1.19.1-scala_2.12-java17

COPY --from=build /build/target/flink-lab-job-1.0.0.jar /opt/flink/usrlib/flink-lab-job.jar
