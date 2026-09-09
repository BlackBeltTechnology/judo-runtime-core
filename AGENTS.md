# judo-runtime-core — module agent doctrine

## Module purpose

`judo-runtime-core` is the execution substrate a generated JUDO application
actually runs on: given the models a JUDO project produces (ASM, RDBMS,
expression, query, measure, liquibase, keycloak), this reactor supplies the
running machinery that turns those models into a live, transactional,
access-controlled service — no generated business code is emitted here, the
models are interpreted at runtime.

Six pillars carry that work, and they stack in this order:

- **Context and core abstractions** — `DataTypeManager` type coercion,
  identifier providers, metrics/cancellation tokens, payload traversal, and the
  runtime exception vocabulary. Everything below binds to these.
- **Query** — resolves JQL expressions against the ASM/expression metamodels
  into a database-agnostic query model (feature-by-feature translation, plus
  aggregation handling). It answers *what* to read, never *how*.
- **DAO / RDBMS layer** — the persistence engine. `dao-core` holds the
  model-driven CRUD contract (statement collection, value resolution, insert /
  update / delete processors); `dao-rdbms` turns the query model into real SQL
  via translators, mappers and statement executors, honouring a pluggable
  `Dialect`. HSQLDB and PostgreSQL supply the dialects; Liquibase supplies the
  schema the DAO expects to find.
- **Dispatcher** — the operation front door. It routes an inbound operation
  call to a bound behaviour (the generated CRUD/relation behaviours), applies
  the query mask, runs validation, resolves environment variables and
  sequences, and manages the transaction boundary. Consumers reach the runtime
  through the dispatcher, not through the DAO.
- **Access management, security, validation** — orthogonal gates the dispatcher
  invokes. `accessmanager-api` declares the permission contract, `accessmanager`
  decides per-behaviour authorisation, `security` holds the actor/token
  abstractions, the `security-keycloak*` modules realise them against Keycloak,
  and `validator` enforces model-declared constraints before a write lands.
- **Bootstrap and transport** — Guice and Spring Boot wirings that assemble all
  of the above into a runnable application, plus the JAX-RS / CXF / Jetty /
  Jackson layer that exposes it over HTTP.

What a consuming application gets: pick one DI stack (`*-guice-*` or
`*-spring-*`) and one database (`*-hsqldb` for dev, `*-postgresql` for
production), hand it the JUDO models, and it obtains a working DAO, dispatcher,
REST surface and security enforcement without writing persistence or routing
code itself.

Reactor coordinates: `hu.blackbelt.judo.runtime:judo-runtime-core-parent`,
packaging `pom`, Java 21, EPL-2.0, upstream
`https://github.com/BlackBeltTechnology/judo-runtime-core`.

## Reactor map

The root `pom.xml` declares its `<modules>` inside the `modules` profile, which
is active whenever `skipModules` is not `true` — so a plain build descends into
all 31 modules; `-DskipModules=true` builds the parent alone.

| Module | Role |
|---|---|
| `judo-runtime-core` | Bundle. Runtime-wide abstractions every other module binds to: type conversion, identifier providers, metrics + cancellation, payload traversal, exception vocabulary. |
| `judo-runtime-core-dao-core` | Bundle. Model-driven CRUD contract — collects statements from a payload graph, resolves values, orders insert/update/delete processing against the ASM↔RDBMS mapping. Database-neutral. |
| `judo-runtime-core-validator` | Bundle. Enforces model-declared constraints (required, range, pattern, uniqueness) before a write reaches the DAO; raises structured validation faults the dispatcher surfaces. |
| `judo-runtime-core-dao-rdbms` | Bundle. The SQL engine: translates the query model into dialect-parameterised SQL through translators/mappers/join models and runs it via statement executors. Defines the `Dialect` seam. |
| `judo-runtime-core-dao-rdbms-hsqldb` | Bundle. HSQLDB dialect — in-memory/embedded target for development and tests, including HSQLDB-specific query mappers. |
| `judo-runtime-core-dao-rdbms-postgresql` | Bundle. PostgreSQL dialect for production, adding PostgreSQL query mappers, sequence support and Liquibase snippets. |
| `judo-runtime-core-dao-rdbms-liquibase` | Bundle. Applies the generated Liquibase changelog so the physical schema matches the RDBMS model the DAO assumes. |
| `judo-runtime-core-expression` | Bundle. Builds JQL expressions against the ASM + measure adapters into the expression model the query layer consumes. |
| `judo-runtime-core-query` | Bundle. Feature-by-feature translation of expressions into the database-agnostic query model, including aggregated features. Decides *what* to read. |
| `judo-runtime-core-dispatcher` | Bundle. Operation entry point: behaviour binding, query masking, environment/sequence resolution, transaction boundary, security handoff. Carries the ANTLR grammar for query masks. |
| `judo-runtime-core-accessmanager-api` | Bundle. Zero-dependency permission contract — the interface the dispatcher calls to ask "may this actor run this operation". |
| `judo-runtime-core-accessmanager` | Bundle. Default decision engine implementing that contract per generated behaviour. |
| `judo-runtime-core-security` | Bundle. Actor/token/claim abstractions and the identifier-to-actor resolution the dispatcher relies on; identity-provider-neutral. |
| `judo-runtime-core-security-keycloak` | Bundle. Realises the security abstractions against a Keycloak realm driven by the keycloak metamodel, with resilience4j-guarded admin calls. |
| `judo-runtime-core-security-keycloak-cxf` | Bundle. Binds the Keycloak adapter into the CXF request pipeline so a JAX-RS call arrives already authenticated. |
| `judo-runtime-core-jaxrs-cxf-server` | Bundle. CXF JAX-RS server bootstrap — publishes the model-derived endpoints on a CXF frontend with HTTP transport. |
| `judo-runtime-core-jackson` | Bundle. JSON codec for runtime payloads: serializers/deserializers for JUDO types plus the JSR-310/JSR-353/JDK8 datatype registrations. |
| `judo-runtime-core-jaxrs` | Bundle. Container-neutral JAX-RS providers mapping runtime faults and payloads onto HTTP semantics. |
| `judo-runtime-core-jaxrs-cxf` | Bundle. CXF-specific interceptors layered on those providers. |
| `judo-runtime-core-export-jxls` | Bundle. Renders dispatcher results into Excel workbooks via JXLS/POI for model-declared export operations. |
| `judo-runtime-core-guice` | Jar. The Guice assembly of the whole stack — core, DAO/RDBMS, dispatcher, accessmanager and security modules wired into one injector. |
| `judo-runtime-core-guice-hsqldb` | Jar. Guice datasource wiring for HSQLDB (HikariCP + Atomikos JDBC transactions). |
| `judo-runtime-core-guice-postgresql` | Jar. Guice datasource wiring for PostgreSQL; tests run against a TestContainers Postgres. |
| `judo-runtime-core-guice-jetty` | Jar. Embedded Jetty server module so a Guice-assembled runtime serves HTTP standalone. |
| `judo-runtime-core-guice-cxf` | Jar. Joins Guice, Jetty and CXF: JAX-RS frontend, CORS, logging/metrics features and Jackson providers on the injected runtime. |
| `judo-runtime-core-guice-keycloak` | Jar. Adds Keycloak-backed security to the Guice+CXF stack; integration-tested against a Keycloak container. |
| `judo-runtime-core-dependencies` | Pom. Dependency-management BOM pinning every runtime artifact and third-party version, so consumers import one coordinate instead of aligning versions by hand. |
| `judo-runtime-core-spring` | Jar. Spring Boot autoconfiguration of the runtime — beans for DAO, dispatcher, accessmanager and security, driven by application properties. |
| `judo-runtime-core-spring-hsqldb` | Jar. Spring Boot autoconfiguration binding the runtime to an HSQLDB datasource. |
| `judo-runtime-core-spring-postgresql` | Jar. Spring Boot autoconfiguration binding the runtime to a PostgreSQL datasource. |
| `judo-runtime-core-guice-testkit` | Jar. Reusable test fixtures and harness for exercising a fully injected runtime — model fixtures, dispatcher/DAO assertions, container and stdout/env stubbing. |

Packaging is not decorative: `bundle` modules are OSGi bundles built by the
Felix bundle plugin and must keep their exported-package surface sane;
integration/wiring modules (`guice-*`, `spring-*`, `jaxrs-cxf*`,
`security-keycloak*`, `export-jxls`) are plain jars; `-dependencies` is a
`pom`-only BOM.

## Build commands

Prerequisites: Java 21+, Maven 3.8.3+. Prefer the bundled wrapper (`./mvnw`).

```bash
# Full build with tests
mvn clean install

# Build without tests
mvn clean install -DskipTests

# Build without modules (parent only)
mvn clean install -DskipModules=true

# Run tests with coverage
mvn clean verify

# Generate Javadoc
mvn javadoc:javadoc

# SonarQube analysis
mvn sonar:sonar
```

### Testing

```bash
# All tests
mvn test

# Integration tests only
mvn verify

# Skip tests
mvn install -DskipTests

# Run specific test
mvn test -Dtest=ClassName
```

Test stack: JUnit 5 (Jupiter) 5.13.4, Mockito 5.17.0, Hamcrest 2.2, and
TestContainers 1.21.1 — the containerised suites bring up PostgreSQL and
Keycloak, so Docker must be available for `verify` on `guice-postgresql`,
`guice-cxf` and `guice-keycloak`. Test logging is configured by the
`logback-test.xml` in the reactor root.

### Release and utility profiles

```bash
# Sign artifacts (for releases)
mvn clean deploy -Psign-artifacts

# Deploy to Nexus (judong)
mvn clean deploy -Prelease-judong

# Deploy to Maven Central
mvn clean deploy -Prelease-central

# Local file-based deployment
mvn clean deploy -Prelease-dummy

# Update license headers
mvn license:format -Pupdate-source-code-license

# Generate documentation diagrams
mvn -Pgenerate-github-asciidoc-diagrams
```

| Profile | Effect |
|---|---|
| `modules` | Active unless `-DskipModules=true`. Supplies the 31-module `<modules>` list. |
| `sign-artifacts` | GPG-signs the produced artifacts. |
| `release-dummy` | Redirects distributionManagement to a local file URL for dry runs. |
| `release-judong` | Deploys to the judong Nexus. |
| `release-central` | Deploys to OSSRH / Maven Central. |
| `generate-github-asciidoc-diagrams` | Renders the AsciiDoc HTML docs and copies the generated gitflow diagrams. |
| `update-source-code-license` | Rewrites EPL-2.0 license headers across the sources. |

## Quick start

1. Clone and enter the repository:
   ```bash
   git clone https://github.com/BlackBeltTechnology/judo-runtime-core.git
   cd judo-runtime-core
   ```
2. Build it:
   ```bash
   ./mvnw clean install
   ```
3. Choose an integration framework — `judo-runtime-core-guice-*` for Guice,
   `judo-runtime-core-spring-*` for Spring Boot.
4. Choose a database — HSQLDB (`*-hsqldb`) for development, PostgreSQL
   (`*-postgresql`) for production.

## Technology stack

| Category | Technology | Version |
|---|---|---|
| JVM | Java | 21 (LTS) |
| Build | Maven | 3.8.3+ |
| Codegen | Lombok | 1.18.38 |
| DI | Google Guice | 5.1.0 |
| DI | Spring Framework | 6.2.7 |
| DI | Spring Boot | 3.5.0 |
| Web | Apache CXF | 3.6.2 |
| Web | Jetty | 10.0.24 |
| Web | Jakarta WS-RS API | 2.1.6 |
| Persistence | PostgreSQL Driver | 42.7.7 |
| Persistence | HSQLDB | 2.6.1 |
| Persistence | HikariCP | 5.0.0 |
| Persistence | Atomikos (XA) | 5.0.9 |
| Persistence | Liquibase | 4.9.1 |
| JSON | Jackson (+ jaxrs-json-provider, datatype-jsr310) | 2.17.2 |
| Security | Keycloak | 17.0.1 |
| Security | JOSE4j (JWT) | 0.7.2 |
| Expression | ANTLR | 4.5.1-1 |
| Expression | Epsilon Runtime | 2.8.0 |
| Testing | JUnit 5 (Jupiter) | 5.13.4 |
| Testing | Mockito | 5.17.0 |
| Testing | TestContainers | 1.21.1 |
| Testing | Hamcrest | 2.2 |
| Utility | Google Guava | 30.0-jre |
| Utility | Commons Lang3 | 3.6 |
| Utility | Reflections | 0.10.2 |
| Utility | Resilience4j | 2.0.2 |

Maven properties fix `UTF-8` source encoding and source/target level 21.

Metamodels consumed from the JUDO ecosystem: `judo-meta-asm` (abstract syntax),
`judo-meta-rdbms` (relational model), `judo-meta-expression`, `judo-meta-query`,
`judo-meta-liquibase` (schema), `judo-meta-keycloak` (IAM), `judo-meta-measure`
(measurement). Other ecosystem dependencies: `judo-dao-api` and
`judo-dispatcher-api` (interface contracts), `judo-sdk-common`, and
`judo-tatami-*` model transformations. External services this runtime talks to:
Keycloak, PostgreSQL/HSQLDB, Jetty, Apache CXF.

## CI/CD

GitHub Actions workflows live under `.github/workflows/`: the main CI pipeline
runs on pushes to `develop` and on PRs; a separate pipeline handles Dependabot
PRs; version bumping runs manually or on schedule; a release is created on push
to `master`, and release publication is triggered manually. Jobs run on the
self-hosted `judong` runner with a 30-minute timeout on Zulu JDK 21.

## Branching and commit policy

| Branch | Purpose |
|---|---|
| `develop` | Latest development version |
| `master` | Latest released version |
| `feature/JNG-*` | Feature development |
| `bugfix/JNG-*` | Bug fixes |
| `release/*` | Release preparation |
| `increment/*` | Version increments |

**Commit requirement:** every commit message must carry a JIRA ticket number
(`JNG-xxx`).

## Working agreements

1. Think the problem through first; read the relevant files in the codebase
   before proposing anything.
2. Check in before any major change and let the plan be verified.
3. Give a high-level explanation of what changed at every step.
4. Keep every task and code change as simple as possible — no massive or
   complex changes, minimal blast radius. Simplicity governs.
5. Maintain a documentation file describing how the architecture works, inside
   and out.
6. Never speculate about code you have not opened. If a specific file is
   referenced, read it before answering. Investigate the relevant files before
   answering questions about the codebase; make no claim about code without
   investigating unless certain. Grounded, hallucination-free answers only.
7. Implement with TDD: write or update the tests first to define expected
   behaviour, verify they fail, then write the minimal implementation that
   makes them pass.
8. Apply DRY: extract reusable logic into separate classes, utilities or
   components; when the same pattern appears in several places, refactor it
   into a shared helper.

See `README.adoc` for project documentation and `CONTRIBUTING.adoc` for the
contribution process. Licensed under EPL-2.0 (`LICENSE.txt`).

<!-- dox-doctrine -->
## Documentation Update Protocol (WRITE discipline)

Per-directory `AGENTS.md` files form a tree. Each directory `AGENTS.md` is the
per-file record for the files in that directory. This module-root `AGENTS.md`
holds doctrine + architecture pointers only — never a per-file index.

**Keep the root lean.** This file loads into every agent turn — every byte costs
tokens on every turn. A verbose root file buries the rules the model must follow
(signal dilution) and measurably degrades adherence; a lean file keeps doctrine
salient. Default assumption: your update does NOT belong in the root — route it
by the table below.

**Route every doc update by kind:**

| Kind of update | Goes in |
|---|---|
| New file in a directory, or its per-file detail / change history | Nearest directory `AGENTS.md`. Add a `` | `<basename>` | <purpose> | `` row, path-alphabetical. |
| Data flow, protocol, architecture rationale | `docs/architecture.md` or a `docs/<topic>.md` |
| End-user / developer setup | `README.md` |
| Cross-cutting rule every agent needs every turn (rare) | this module-root `AGENTS.md` |

**Read before editing (chain walk).** Before editing a file, read the nearest
`AGENTS.md` chain root→leaf so you know the file's recorded purpose, contracts,
and change history. Do not edit blind.

**Update after editing (closeout pass).** After changing a file, update its row
in the nearest directory `AGENTS.md`: find the file's row, update its purpose in
place; if absent, add it in path-alphabetical order. New directory → scaffold
its `AGENTS.md`. One row per file. The purpose carries a one-line summary, key
exported symbols, contracts/invariants, and `See change: <id>` history.

**Row style (caveman).** Short declarative fragments. Drop articles. Subject →
verb → object, present tense. One fact per row. Prefer concrete tokens (paths,
symbols, env vars) over prose. Keep identifiers verbatim.

**Size rule — split an over-large directory `AGENTS.md` file-based.** pi
auto-injects a directory `AGENTS.md` on every turn when cwd sits at/below it, so
an over-large directory `AGENTS.md` is not supported. Split it file-based: a row
exceeding the length threshold promotes to a per-file `<File>.AGENTS.md`
sidecar carrying that file's full detail (including every `See change:`). The
sidecar is pull-only — its name is not `AGENTS.md`, so pi never auto-injects it
— yet it stays search-indexed (`agents` doc_type). The directory `AGENTS.md`
keeps a one-line summary plus a `→ see `<File>.AGENTS.md`` pointer. Rows within
the threshold stay verbatim (lossless).

## Finding docs (READ discipline)

`kb_*` tools are faster and cheaper than raw search — they return a one-line
purpose + key exports per file, not raw bytes. **This fires on the ACTION, not
the intent** — before you `grep`/`rg` for a symbol, `cat`/read a file to learn
what it does, or chase an import, the kb call goes first. It fires **even
mid-task when you already know the file**; knowing the file does not exempt you.
When your reflex is the left column, run the right column instead:

| You're about to… | Do this FIRST instead |
|---|---|
| `grep -rn "SymbolName" src/` — find where a fn / type / const lives | `kb_search --doc-type agents "SymbolName"` — tree indexes key exports per file |
| `grep -rn "feature\|topic" src/` — how does X work / where's X handled | `kb_search "feature topic"` |
| `cat` / read a file just to learn its purpose before editing | `kb agents <path>` — one-line purpose + exports + change history |
| chase imports / callers across files | `kb_neighbors <path\|heading>` |
| read one doc section in full | `kb_get <path> <section>` |

**Fall-through (explicit):** if the kb call returns nothing relevant, `rg` /
source read is allowed — then add the missing directory `AGENTS.md` row per the
WRITE discipline. kb does NOT replace grep; it goes first.
