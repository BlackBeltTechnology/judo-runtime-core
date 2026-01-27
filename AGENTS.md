# JUDO Runtime Core - Developer Guide

## Project Overview

**Project:** JUDO Runtime Core  
**GroupId:** `hu.blackbelt.judo.runtime`  
**ArtifactId:** `judo-runtime-core-parent`  
**Version:** 1.0.6-SNAPSHOT  
**License:** Eclipse Public License 2.0 (EPL-2.0)  
**Repository:** https://github.com/BlackBeltTechnology/judo-runtime-core

JUDO Runtime Core is an enterprise-grade Java runtime framework providing data access, business logic, security, and API services for JUDO-based applications.

---

## Technology Stack

### Core Technologies

| Category | Technology | Version |
|----------|------------|---------|
| **JVM** | Java | 21 (LTS) |
| **Build** | Maven | 3.8.3+ |
| **Code Generation** | Lombok | 1.18.38 |

### Dependency Injection

| Framework | Version | Usage |
|-----------|---------|-------|
| Google Guice | 5.1.0 | Primary DI container |
| Spring Framework | 6.2.7 | Alternative framework |
| Spring Boot | 3.5.0 | Autoconfiguration |

### Web & API

| Technology | Version | Purpose |
|------------|---------|---------|
| Apache CXF | 3.6.2 | JAX-RS/SOAP services |
| Jetty | 10.0.24 | Servlet container |
| Jakarta WS-RS API | 2.1.6 | JAX-RS standards |

### Database & Persistence

| Technology | Version | Purpose |
|------------|---------|---------|
| PostgreSQL Driver | 42.7.7 | PostgreSQL support |
| HSQLDB | 2.6.1 | In-memory/embedded DB |
| HikariCP | 5.0.0 | Connection pooling |
| Atomikos | 5.0.9 | XA transactions |
| Liquibase | 4.9.1 | Schema migrations |

### JSON Processing

| Library | Version |
|---------|---------|
| Jackson | 2.17.2 |
| jackson-jaxrs-json-provider | 2.17.2 |
| jackson-datatype-jsr310 | 2.17.2 |

### Security

| Technology | Version | Purpose |
|------------|---------|---------|
| Keycloak | 17.0.1 | OAuth2/OpenID Connect |
| JOSE4j | 0.7.2 | JWT support |

### Expression & Query

| Technology | Version | Purpose |
|------------|---------|---------|
| ANTLR | 4.5.1-1 | Parser/lexer generation |
| Epsilon Runtime | 2.8.0 | Model transformation |

### Metamodels (JUDO Ecosystem)

| Model | Purpose |
|-------|---------|
| judo-meta-asm | Abstract Syntax Model |
| judo-meta-rdbms | Relational Database Model |
| judo-meta-expression | Expression Model |
| judo-meta-query | Query Model |
| judo-meta-liquibase | Schema Model |
| judo-meta-keycloak | IAM Model |
| judo-meta-measure | Measurement Model |

### Testing

| Library | Version | Purpose |
|---------|---------|---------|
| JUnit 5 (Jupiter) | 5.13.4 | Unit testing |
| Mockito | 5.17.0 | Mocking |
| TestContainers | 1.21.1 | Integration testing |
| Hamcrest | 2.2 | Assertions |

### Utility Libraries

| Library | Version |
|---------|---------|
| Google Guava | 30.0-jre |
| Commons Lang3 | 3.6 |
| Reflections | 0.10.2 |
| Resilience4j | 2.0.2 |

---

## Build Commands

### Prerequisites
- Java 21+
- Maven 3.8.3+

### Basic Build

```bash
# Full build with tests
mvn clean install

# Build without tests
mvn clean install -DskipTests

# Build without modules (parent only)
mvn clean install -DskipModules=true
```

### Code Quality

```bash
# Run tests with coverage
mvn clean verify

# Generate Javadoc
mvn javadoc:javadoc

# SonarQube analysis
mvn sonar:sonar
```

### Release Profiles

```bash
# Sign artifacts (for releases)
mvn clean deploy -Psign-artifacts

# Deploy to Nexus (judong)
mvn clean deploy -Prelease-judong

# Deploy to Maven Central
mvn clean deploy -Prelease-central

# Local file-based deployment
mvn clean deploy -Prelease-dummy
```

### Utility Profiles

```bash
# Update license headers
mvn license:format -Pupdate-source-code-license

# Generate documentation diagrams
mvn -Pgenerate-github-asciidoc-diagrams
```

---

## Project Structure

```
judo-runtime-core/
├── pom.xml                          # Parent POM (aggregator)
├── README.adoc                      # Project documentation
├── CONTRIBUTING.adoc                # Contribution guidelines
├── LICENSE.txt                      # EPL-2.0 license
├── logback-test.xml                 # Test logging config
├── .mvn/                            # Maven wrapper
├── .github/workflows/               # CI/CD pipelines
└── [31 module directories]
```

---

## Module Architecture

### Module Dependency Flow

```
                    ┌─────────────────────────────────┐
                    │    judo-runtime-core            │
                    │    (Core Abstractions)          │
                    └───────────────┬─────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        │                           │                           │
        ▼                           ▼                           ▼
┌───────────────┐         ┌─────────────────┐         ┌─────────────────┐
│  dao-core     │         │   expression    │         │    security     │
│ (DAO Layer)   │         │  (Expressions)  │         │  (Auth Base)    │
└───────┬───────┘         └─────────────────┘         └────────┬────────┘
        │                                                       │
        ▼                                                       ▼
┌───────────────┐                                     ┌─────────────────┐
│  dao-rdbms    │                                     │security-keycloak│
│ (RDBMS Impl)  │                                     │(Keycloak Auth)  │
└───────┬───────┘                                     └─────────────────┘
        │
        ├─────────────────┬─────────────────┐
        ▼                 ▼                 ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│dao-rdbms-hsqldb│ │dao-rdbms-    │ │dao-rdbms-     │
│               │ │postgresql    │ │liquibase      │
└───────────────┘ └───────────────┘ └───────────────┘

                    Integration Frameworks
        ┌───────────────────────────────────────────┐
        │                                           │
        ▼                                           ▼
┌───────────────────────────┐         ┌─────────────────────────┐
│      Guice Stack          │         │      Spring Stack       │
│ ┌─────────────────────┐   │         │ ┌───────────────────┐   │
│ │ guice (core DI)     │   │         │ │ spring (autoconf) │   │
│ │ guice-hsqldb        │   │         │ │ spring-hsqldb     │   │
│ │ guice-postgresql    │   │         │ │ spring-postgresql │   │
│ │ guice-jetty         │   │         │ └───────────────────┘   │
│ │ guice-cxf           │   │         └─────────────────────────┘
│ │ guice-keycloak      │   │
│ │ guice-testkit       │   │
│ └─────────────────────┘   │
└───────────────────────────┘
```

---

## Module Descriptions

### Foundation Modules

| Module | Description |
|--------|-------------|
| `judo-runtime-core` | Core runtime abstractions including DataTypeManager, identifier providers, metrics collectors, and common exceptions |
| `judo-runtime-core-dependencies` | Centralized dependency management (BOM) for consistent versioning |

### Data Access Layer (DAO)

| Module | Description |
|--------|-------------|
| `judo-runtime-core-dao-core` | Core DAO interfaces and abstractions for data access |
| `judo-runtime-core-dao-rdbms` | RDBMS-specific DAO implementations with query translation, statement executors, and result mapping |
| `judo-runtime-core-dao-rdbms-hsqldb` | HSQLDB database dialect provider for development and testing |
| `judo-runtime-core-dao-rdbms-postgresql` | PostgreSQL database dialect provider for production use |
| `judo-runtime-core-dao-rdbms-liquibase` | Liquibase integration for database schema management and migrations |

### Business Logic & Processing

| Module | Description |
|--------|-------------|
| `judo-runtime-core-expression` | Expression evaluation engine for computing dynamic values and conditions |
| `judo-runtime-core-query` | Query processing and translation layer converting abstract queries to SQL |
| `judo-runtime-core-dispatcher` | Request dispatching and operation processing with ANTLR-based parsing |
| `judo-runtime-core-validator` | Data validation framework for enforcing business rules and constraints |

### Security & Access Control

| Module | Description |
|--------|-------------|
| `judo-runtime-core-security` | Core security framework with authentication and authorization abstractions |
| `judo-runtime-core-security-keycloak` | Keycloak OAuth2/OpenID Connect provider integration |
| `judo-runtime-core-security-keycloak-cxf` | Keycloak integration with Apache CXF for JAX-RS security |
| `judo-runtime-core-accessmanager-api` | Access control API defining permission and role abstractions |
| `judo-runtime-core-accessmanager` | Access control implementation with permission checking |

### Serialization & API

| Module | Description |
|--------|-------------|
| `judo-runtime-core-jackson` | Jackson JSON serialization providers and type handlers |
| `judo-runtime-core-jaxrs` | JAX-RS provider implementations for REST APIs |
| `judo-runtime-core-jaxrs-cxf` | Apache CXF JAX-RS integration |
| `judo-runtime-core-jaxrs-cxf-server` | CXF server configuration and bootstrap |

### Guice Integration

| Module | Description |
|--------|-------------|
| `judo-runtime-core-guice` | Core Google Guice dependency injection modules |
| `judo-runtime-core-guice-hsqldb` | Guice + HSQLDB configuration module |
| `judo-runtime-core-guice-postgresql` | Guice + PostgreSQL configuration module |
| `judo-runtime-core-guice-jetty` | Jetty servlet container integration with Guice |
| `judo-runtime-core-guice-cxf` | Guice + Apache CXF integration |
| `judo-runtime-core-guice-keycloak` | Guice + Keycloak security integration |
| `judo-runtime-core-guice-testkit` | Testing toolkit providing utilities for Guice-based testing |
| `judo-runtime-core-guice-dependencies` | Guice dependency management module |

### Spring Integration

| Module | Description |
|--------|-------------|
| `judo-runtime-core-spring` | Spring Boot autoconfiguration for JUDO runtime |
| `judo-runtime-core-spring-hsqldb` | Spring + HSQLDB autoconfiguration |
| `judo-runtime-core-spring-postgresql` | Spring + PostgreSQL autoconfiguration |

### Utilities

| Module | Description |
|--------|-------------|
| `judo-runtime-core-export-jxls` | JXLS-based Excel export functionality using Apache POI |

---

## Key Source Packages

### Core Module
```
hu.blackbelt.judo.runtime.core/
├── DataTypeManager.java           # Type conversion and management
├── UUIDIdentifierProvider.java    # UUID-based ID generation
├── SerializableIdentifierProvider.java
├── MetricsCancelToken.java        # Operation cancellation
├── MetricsCollector.java          # Performance metrics
├── PayloadTraverser.java          # Data traversal utilities
└── exception/                     # Custom exceptions
```

### DAO RDBMS Module
```
hu.blackbelt.judo.runtime.core.dao.rdbms/
├── AbstractRdbmsDAO.java          # Base DAO implementation
├── RdbmsDAOImpl.java              # Full RDBMS DAO
├── RdbmsInit.java                 # Database initialization
├── RdbmsResolver.java             # Entity resolution
├── Dialect.java                   # Database dialect abstraction
├── executors/                     # Statement executors
└── query/                         # Query translation
```

---

## Packaging Strategy

### OSGi Bundle Modules
Most core modules use `<packaging>bundle</packaging>` with Apache Felix Bundle Plugin:
- dao-core, dao-rdbms
- expression, query, dispatcher, validator
- security, jaxrs, jackson

### Standard JAR Modules
Integration and implementation modules use standard JAR packaging:
- guice-*, spring-*
- jaxrs-cxf, security-keycloak-*
- export-jxls

---

## CI/CD Pipeline

### GitHub Actions Workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `build.yml` | Push to develop, PRs | Main CI pipeline |
| `build-dependabot.yml` | Dependabot PRs | Dependency updates |
| `bump-version.yml` | Manual/scheduled | Version bumping |
| `create-release-on-master.yml` | Push to master | Release creation |
| `release.yml` | Manual | Release triggering |

### Build Configuration
- **Runner:** Self-hosted (judong)
- **Timeout:** 30 minutes
- **JDK:** Java 21 (Zulu distribution)

---

## Branching Strategy

| Branch | Purpose |
|--------|---------|
| `develop` | Latest development version |
| `master` | Latest released version |
| `feature/JNG-*` | Feature development |
| `bugfix/JNG-*` | Bug fixes |
| `release/*` | Release preparation |
| `increment/*` | Version increments |

**Commit Requirement:** All commits must include a JIRA ticket number (JNG-xxx)

---

## External Integrations

### JUDO Ecosystem Dependencies
- **judo-dao-api** - DAO interface definitions
- **judo-dispatcher-api** - Dispatcher interface definitions
- **judo-sdk-common** - Common SDK utilities
- **judo-tatami** - Model transformation framework
- **judo-meta-*** - Various metamodels

### External Services
- **Keycloak** - Identity and access management
- **PostgreSQL/HSQLDB** - Database backends
- **Jetty** - HTTP server
- **Apache CXF** - SOAP/REST services

---

## Testing

### Test Stack
- JUnit 5 (Jupiter) for unit tests
- Mockito for mocking
- TestContainers for integration tests
- Hamcrest for assertions

### Running Tests
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

### TestContainers
Used for integration testing with:
- PostgreSQL containers
- Keycloak containers

---

## Configuration

### Logging
Test logging configured via `logback-test.xml` in project root.

### Maven Properties
```xml
<project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
<maven.compiler.source>21</maven.compiler.source>
<maven.compiler.target>21</maven.compiler.target>
```

---

## Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/BlackBeltTechnology/judo-runtime-core.git
   cd judo-runtime-core
   ```

2. **Build the project**
   ```bash
   ./mvnw clean install
   ```

3. **Choose integration framework**
   - For Guice: Use `judo-runtime-core-guice-*` modules
   - For Spring: Use `judo-runtime-core-spring-*` modules

4. **Choose database**
   - Development: HSQLDB (`*-hsqldb` modules)
   - Production: PostgreSQL (`*-postgresql` modules)
