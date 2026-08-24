# Worker(执行器)镜像：多阶段构建
FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /workspace
COPY . .
RUN mvn -B -DskipTests -pl scheduler-worker -am package

FROM eclipse-temurin:11-jre
WORKDIR /app
COPY --from=build /workspace/scheduler-worker/target/scheduler-worker-1.0-SNAPSHOT-all.jar app.jar
ENV JAVA_OPTS=""
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
