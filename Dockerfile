## ---------------------------------------------------------------------------

# Stage 1: Build Stage using SBT Docker Image
FROM datafoundrydocker:basecontainer AS builder

# Set working directory
WORKDIR /app

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
bundle install && \
bundle exec jekyll build --config _config_internal.yml

ARG BUILD_MODE=stage
ENV BUILD_MODE=${BUILD_MODE}

RUN sbt update

# Build the application
RUN sbt dist && ls -l /app/target/universal

## ---------------------------------------------------------------------------

# Stage 2: Production Mode
## to run the dockerfile in production mode:
## docker build --tag datafoundrydocker:production --target production . && docker compose -f DF-production.yaml up

FROM --platform=linux/amd64 ghcr.io/graalvm/graalvm-community:25 AS production

# Set working directory
WORKDIR /app

RUN microdnf install unzip

# Copy the built JAR file from the build stage
COPY --from=builder /app/target/universal/datafoundry-*.zip app.zip

RUN unzip app.zip && mv datafoundry* datafoundry

# Expose the application port
EXPOSE 9000

# Switch to DF directory
WORKDIR /app/datafoundry
CMD ["bin/datafoundry", "-Dplay.evolutions.autoApply=true", "-Dconfig.file=/app/datafoundry/conf/application.conf", "-Dlogger.file=/app/datafoundry/conf/logback.xml"]

# For testing
# CMD ["/bin/bash"]
