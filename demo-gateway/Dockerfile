# Runs the jar built on the host (./mvnw package -pl demo-gateway -am) together with the UI built on the host
# (npm run build in demo-ui/). The build context is the repository root, see .dockerignore:
#   docker build -f demo-gateway/Dockerfile -t demo-gateway .
FROM amazoncorretto:25-alpine AS extract
WORKDIR /build
COPY demo-gateway/target/demo-gateway-*.jar app.jar
# Split the fat jar into layers: dependencies change rarely, so a rebuild after a code change reuses their layer.
RUN java -Djarmode=tools -jar app.jar extract --layers --destination extracted

FROM amazoncorretto:25-alpine
WORKDIR /app
# Fixed uid: the deployment hands the token signing key to exactly this user
RUN addgroup -S -g 10001 app && adduser -S -u 10001 -G app app
COPY --from=extract /build/extracted/dependencies/ ./
COPY --from=extract /build/extracted/spring-boot-loader/ ./
COPY --from=extract /build/extracted/snapshot-dependencies/ ./
COPY --from=extract /build/extracted/application/ ./
COPY demo-ui/dist/ ./ui/
USER app
EXPOSE 8082
# The UI is served from demo.ui-location; the default (file:demo-ui/dist/) only fits a run from the repository root.
ENV DEMO_UILOCATION=file:/app/ui/
ENTRYPOINT ["java", "-jar", "app.jar"]
