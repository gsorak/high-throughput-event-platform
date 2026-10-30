# Runs the jar built on the host (./mvnw package). Used by the "apps" profile of infra/local/compose.yaml.
FROM amazoncorretto:25-alpine AS extract
WORKDIR /build
COPY target/event-processor-*.jar app.jar
# Split the fat jar into layers: dependencies change rarely, so a rebuild after a code change reuses their layer.
RUN java -Djarmode=tools -jar app.jar extract --layers --destination extracted

FROM amazoncorretto:25-alpine
WORKDIR /app
RUN addgroup -S app && adduser -S app -G app
COPY --from=extract /build/extracted/dependencies/ ./
COPY --from=extract /build/extracted/spring-boot-loader/ ./
COPY --from=extract /build/extracted/snapshot-dependencies/ ./
COPY --from=extract /build/extracted/application/ ./
USER app
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
