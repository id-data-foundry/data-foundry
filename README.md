<p align="center">
  <img src="modules/common/public/images/logo.png" alt="DataFoundry Logo" width="200"/>
</p>

<h1 align="center">DataFoundry</h1>

<p align="center">
  <!-- Badges -->
  <a href="#"><img src="https://img.shields.io/badge/build-passing-brightgreen" alt="Build Status"></a>
  <a href="https://www.gnu.org/licenses/agpl-3.0"><img src="https://img.shields.io/badge/license-AGPL_v3-blue.svg" alt="License: AGPL v3"></a>
  <a href="#"><img src="https://img.shields.io/github/issues/data-foundry-id/data-foundry" alt="Open Issues"></a>
  <a href="#"><img src="https://img.shields.io/github/release/data-foundry-id/data-foundry.svg" alt="GitHub release"></a>
</p>

**DataFoundry** is a data infrastructure platform built for the Eindhoven University of Technology Department of Industrial Design. Data Foundry contains tools for data collection and prototyping, aimed at design research and education. This repo contains the main web server component. The webserver has been built ontop of the Play Framework.

With DataFoundry, we aim to ease data collection and processing, making it extremely easy to connect various data sources and combine all data into a single data platform that encourages new forms of design research, mashing up, making and hacking.

## Key Features

*   **Versatile Data Management:** Supports numerous dataset types including IoT (timeseries), Entity (key-value), Media (images), Diaries, Forms, and more.
*   **User & Project Administration:** Full support for managing users, projects, and roles (admins, librarians, moderators). Making it easier to be approved in DPIA procedures.
*   **Authentication:** Secure authentication with Single Sign-On (SSO) via OpenID Connect, Azure or SAML.
*   **Real-time Data Streaming:** Seamless integration with the OOCSI real-time messaging ecosystem and our own API. Making cross-device data collection a breeze.
*   **External Service Integration:** Connects with services like     Rawgraphs, Azure, localAI, OpenAI, Telegram, Fitbit, and Google Fit.
*   **Built-in Tooling:**
    *   **Scripting:** Run (sandboxed) server-side JavaScript to process, filter, and react to data.
    *   **AI Tools:** Integrated support for local and remote AI models (LLMs, Text-to-Speech). Including our own API wrapper to manage tokens.
    *   **Notebooks:** In-browser Python/JavaScript notebooks (Starboard) for data analysis and documentation.
    *   **Transcription:** On-premise audio/video transcription powered by Whisper.
    *   **ESP Tools:** Web-based flashing and file management for ESP32 microcontrollers.
*   **Client Libraries:** Connect prototypes and applications from various platforms, including Python, Processing, JavaScript, and Unity.
*  **FAIR and Easy publishing workflow:** Data Foundry allows you to directly upload projects to zenodo through fairly. Making it easier than ever to share your datasets and projects.

## Tech Stack

*   **Language:** Java, Scala
*   **Framework:** Play Framework
*   **Build Tool:** sbt (Simple Build Tool)
*   **Database:** H2 (in-memory for dev), Ebean ORM, PostgreSQL (for production)
*   **Containerization:** Docker, Docker Compose, Podman
*   **Frontend:** HTMX, jQuery, SASS
*   **Documentation:** Jekyll

## Architecture

The project follows a modular structure:
*   **Root Project:** The main entry point, configuration, and aggregation.
*   **`modules/common`:** Contains the core logic, routes, models, and dependencies (e.g., Pac4j for auth, Apache Jena for RDF, Lucene for search).
* **`modules/common/app`** Contains most of the platform elements, including the data controllers, views and all other modules.
* **`modules/common/public`** Contains all public assets including imported code and frameworks/platforms (like ViperIDE, Starboard and twine). These can be found in the vendor folder.

Routes are split, with the root `conf/routes` delegating to `common.Routes` located in `modules/common/conf/`. This modularity allows for clear separation of concerns.

## Getting Started

### Prerequisites
*   Docker & Docker Compose (or Podman)
*   Git

### Docker/Podman (Recommended)

The easiest way to get DataFoundry up and running is with Docker. Internally we use Podman for development as this comes with additional security benefits, but Data Foundry should also be fully compatible with docker.

**1. Clone the Repository**
```bash
# Clone this repository
git clone https://github.com/data-foundry-id/data-foundry.git
cd data-foundry

# Initialize and update submodules
git submodule init
git submodule update
```

**2. Build the Base Image**

  ```bash
  docker build --tag datafoundrydocker:basecontainer -f .devcontainer/Dockerfile.base .
  ```

**3. Run in Development Mode**
This command builds the development image and starts the application using Docker Compose.
```bash
docker build --tag datafoundrydocker:development --target development . && docker compose -f DF-development.yaml up
```
The application will be available at `http://localhost:9000`. The environment exposes ports `9000` (App), `8001`, and `9092`.

**4. Run in Production Mode**
For a production deployment, use the following command:
```bash
docker build --tag datafoundrydocker:production --build-arg BUILD_MODE=stage --target production . && docker compose -f DF-production.yaml up
```

### Local Development (sbt)

You can also run the application directly on your host machine.

*   **Prerequisites:** Java 25+, sbt
*   **Configure:** Edit `conf/application.conf` with your desired settings. For more information on configuration options, see the [Configuration Documentation](docs/Configuration.md).
*   **Run:**
    ```bash
    sbt run
    ```

## Usage & Examples

Once running, you can interact with DataFoundry through its web interface or programmatically via its API. The `documentation/` directory contains extensive guides, tutorials, and examples for everything from connecting your first datalogger to building a complete AI-powered chatbot.

## API Documentation

DataFoundry exposes a comprehensive REST API for programmatic access.
*   **Swagger UI:** A live, interactive API documentation is available in a running instance under the `/public/lib/swagger-ui/` path.
*   **Swagger Definition:** The OpenAPI (Swagger) definition can be found in `conf/swagger.yml`.
*   **Reference Docs:** Further details on the API can be found in the `documentation/_Reference` directory.

## Contributing

Contributions from the community are welcome! Whether it's reporting a bug, proposing a new feature, or submitting a pull request, we appreciate your help.

Please see our [`CONTRIBUTING.md`](CONTRIBUTING.md) file for detailed guidelines.

## License

This project is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.

In short, this means you are free to use, study, share, and modify the software. If you run a modified version of this software on a network server and let other users interact with it, you must also make your modified source code available to them.

For the full license text, see the `LICENSE` file.

---

## Roadmap for Open-Source Readiness

This is a checklist for the development team to prepare the repository for its public release on GitHub.

### Governance & Community
- [x] Choose and add a root `LICENSE` file (e.g., Apache 2.0, MIT).
- [x] Create a `CONTRIBUTING.md` detailing contribution workflows.
- [x] Create a `CODE_OF_CONDUCT.md`.
- [] Migrate `.gitlab/issue_templates` to `.github/ISSUE_TEMPLATE`.
- [x] Add a `SECURITY.md` file with a vulnerability disclosure policy.

### CI/CD & Automation
- [ ] Set up GitHub Actions to replace `.gitlab-ci.yml`.
- [ ] Create a workflow to run `sbt test`.
- [ ] Create a workflow to build Docker images for development and production.
- [ ] (Optional) Create a release workflow to publish Docker images.

### Codebase & Configuration
- [ ] Audit all `*.conf` files for secrets and internal URLs. Replace with placeholders or environment variable references.
- [ ] Verify that test configurations (`tests.conf`) use mock keys and are safe for public view.
- [ ] Remove any dead code or internal-only modules not intended for release.

### Documentation
- [ ] Perform a full audit of the `documentation/` folder.
- [ ] Replace internal links with relative links or links to the future public documentation site.
- [ ] Remove any internal-only guides or references.
- [ ] Update `documentation/_config.yml` to remove any internal build configurations.