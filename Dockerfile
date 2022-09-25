FROM airbyte/worker:0.40.4

ARG DOCKER_BUILD_ARCH=amd64

ARG VERSION=0.40.4

WORKDIR /app

# Move worker app
ADD target/airbyte-do.jar /app
ADD airbyte-workers /app/airbyte-workers-0.40.4/bin

RUN chmod a+x /app/airbyte-workers-0.40.4/bin/airbyte-workers

ENV JAVA_OPTS -javaagent:/app/airbyte-do.jar -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:9044
EXPOSE 9044

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "${APPLICATION}-${VERSION}/bin/${APPLICATION}"]
