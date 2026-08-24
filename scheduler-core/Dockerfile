# 调度中心镜像：多阶段构建
#   build 阶段：maven 打可执行 fat-jar
#   run 阶段：JRE 直接跑
FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /workspace
# 构建上下文是仓库根目录（docker-compose 里 build.context=.）
COPY . .
# -am 连带构建依赖模块 scheduler-common；shade 插件产出 -all.jar
RUN mvn -B -DskipTests -pl scheduler-core -am package

FROM eclipse-temurin:11-jre
WORKDIR /app
COPY --from=build /workspace/scheduler-core/target/scheduler-core-1.0-SNAPSHOT-all.jar app.jar
# 端口/地址全部由环境变量注入（见 docker-compose.yml）
ENV JAVA_OPTS=""
EXPOSE 8080 8081
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
