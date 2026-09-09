# Stage 1: Build Stage

# Base build
FROM ubuntu:latest AS builder

# Set PATH
WORKDIR /app

ARG DEBIAN_FRONTEND=noninteractive
# Install system dependencies needed for sbt
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    zip \
    systemd \
    gnupg \
    bash \
    npm \
    file \
    python3-libsass \
    build-essential \
    ruby \
    ruby-dev \
    bundler \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install SDKman
RUN curl -s "https://get.sdkman.io?ci=true&rcupdate=false" | bash

# Activate SDKman
RUN /bin/bash -c "source /root/.sdkman/bin/sdkman-init.sh \
    && sdk version \
    && sdk install java 25.0.1-graalce \
    && sdk install sbt"

ENV PATH="/root/.sdkman/candidates/java/current/bin:/root/.sdkman/candidates/sbt/current/bin:/root/.sdkman/candidates/scala/current/bin:${PATH}"

# Test java
RUN java --version

# Test sbt
RUN sbt -version

# Copy SBT project files
COPY build.sbt .
COPY conf ./conf
COPY modules ./modules
COPY project ./project
COPY public ./public
COPY dist ./dist
COPY documentation ./documentation
COPY lib ./lib

RUN cd ./documentation && \
    bundle config set jobs 1 && \
    bundle install --verbose && \
    bundle exec jekyll build --config _config_internal.yml --verbose

ARG BUILD_MODE=stage
ENV BUILD_MODE=${BUILD_MODE}

RUN sbt update

# Build the application
RUN sbt dist && ls -l /app/target/universal

## ---------------------------------------------------------------------------

# Stage 2: Application container

FROM ghcr.io/graalvm/graalvm-community:25 AS production

# Set working directory
WORKDIR /app

# Copy the built JAR file from the build stage
COPY --from=builder /app/target/universal/datafoundry-*.zip app.zip

# Unpack application and rename
RUN microdnf install unzip && \
    unzip app.zip && \
    mv datafoundry* datafoundry && \
    rm app.zip && \
    microdnf remove unzip

# Expose the application port
EXPOSE 9000

# Switch to DF directory
WORKDIR /app/datafoundry
CMD ["bin/datafoundry", "-Dplay.evolutions.autoApply=true", "-Dconfig.file=/app/datafoundry/conf/application.conf", "-Dlogger.file=/app/datafoundry/conf/logback.xml"]

## ---------------------------------------------------------------------------

# use for testing
# CMD ["/bin/bash"]

## to run the dockerfile in production mode:
## docker build --tag datafoundrydocker:production --target production . && docker compose -f DF-production.yaml up
