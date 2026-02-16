
# Data Foundry dev container for local development

This guide describes how to set up and use the Data Foundry development container, in this guide wrriten for Podman.

## Build container

Build the development image. This image contains all the necessary dependencies (such as Java 25 and sbt) required to build and run the project.

```bash
# --tag: Names the image for easy reference
# -f: Specifies the development Dockerfile
podman build --tag datafoundrydocker:basecontainer -f .devcontainer/Dockerfile.base .
podman build --tag datafoundrydocker:development -f .devcontainer/Dockerfile.dev .
```

## Start container

Run the container and mount your local source code into it. This allows you to edit code on your host machine while compiling and running it inside the container.

```bash
# -it: Interactive terminal
# --rm: Automatically remove the container when it exits
# -p 9000:9000: Map the Play Framework application port
# -p 8001:8001: Map the debugger or additional service port
# -v "$(pwd)":/app:z: Mount the current directory (project root) to /app in the container
podman run -it --rm -p 9000:9000 -p 8001:8001 -v "$(pwd)":/app:z datafoundrydocker:development
```

*Note: Ensure you are in the root of the DataFoundry repository when running this command so `$(pwd)` points to the correct location.*

## Local Development
Inside the container (at the `/app` directory), use `sbt` (Scala Build Tool) to manage the project.

### sbt shell

Start the interactive sbt shell. This is recommended for faster subsequent commands as it keeps the JVM running.

```bash
sbt
```

### Dependencies

Download and update the project dependencies defined in the build configuration.

```bash
sbt update
```

### Compile

Compile both Java and Scala source files.

```bash
sbt compile
```

### Test

Execute the project's test suite (JUnit).

```bash
sbt test
```

### Run

To run the application in development mode with hot-reloading enabled:

```bash
sbt run
```
