# JUDO Runtime Core

[![Build](https://github.com/BlackBeltTechnology/judo-runtime-core/actions/workflows/build.yml/badge.svg?branch=develop)](https://github.com/BlackBeltTechnology/judo-runtime-core/actions/workflows/build.yml)

## Introduction

JUDO Runtime Core is the backend runtime framework for the [JUDO platform](https://github.com/BlackBeltTechnology/judo-community). It provides the full server-side stack — data access, business logic dispatching, security, validation, query translation, and REST API serving — all driven by JUDO metamodels (ASM, RDBMS, Expression, Query, etc.).

The framework ships two parallel dependency injection stacks: **Google Guice** for standalone/embedded deployments and **Spring Boot** for Spring-based applications. Both stacks wire the same core modules with database-specific dialects for HSQLDB (development/testing) and PostgreSQL (production).

## Module Architecture

The 31 modules form a layered dependency graph. Core abstractions sit at the top, with concrete implementations and integration modules below.

```mermaid
graph TD
    CORE[judo-runtime-core<br/>Core Abstractions]

    CORE --> DAO_CORE[dao-core<br/>DAO Interfaces]
    CORE --> EXPR[expression<br/>Expression Engine]
    CORE --> SEC[security<br/>Auth Abstractions]

    DAO_CORE --> VALID[validator<br/>Business Rules]
    DAO_CORE --> DAO_RDBMS[dao-rdbms<br/>RDBMS Implementation]

    DAO_RDBMS --> HSQLDB_D[dao-rdbms-hsqldb<br/>HSQLDB Dialect]
    DAO_RDBMS --> PG_D[dao-rdbms-postgresql<br/>PostgreSQL Dialect]
    DAO_RDBMS --> LIQ[dao-rdbms-liquibase<br/>Schema Migrations]

    EXPR --> QUERY[query<br/>Query Translation]
    QUERY --> DISP[dispatcher<br/>Request Routing]

    SEC --> SEC_KC[security-keycloak<br/>Keycloak Provider]
    SEC_KC --> SEC_KC_CXF[security-keycloak-cxf<br/>CXF Integration]

    CORE --> AM_API[accessmanager-api<br/>Access Control API]
    AM_API --> AM[accessmanager<br/>Permission Checking]

    CORE --> JACK[jackson<br/>JSON Serialization]
    JACK --> JAXRS[jaxrs<br/>REST Providers]
    JAXRS --> JAXRS_CXF[jaxrs-cxf<br/>CXF JAX-RS]
    JAXRS_CXF --> JAXRS_CXF_SRV[jaxrs-cxf-server<br/>CXF Server]

    CORE --> EXPORT[export-jxls<br/>Excel Export]

    subgraph Guice Stack
        GUICE[guice<br/>Core DI Module]
        GUICE_HSQLDB[guice-hsqldb]
        GUICE_PG[guice-postgresql]
        GUICE_JETTY[guice-jetty]
        GUICE_CXF[guice-cxf]
        GUICE_KC[guice-keycloak]
        GUICE_TEST[guice-testkit]
    end

    subgraph Spring Stack
        SPRING[spring<br/>Autoconfiguration]
        SPRING_HSQLDB[spring-hsqldb]
        SPRING_PG[spring-postgresql]
    end
```

## Key Runtime Flow

This sequence shows the primary request processing path from an incoming REST call through the dispatcher to the database and back.

```mermaid
sequenceDiagram
    participant Client
    participant JAX-RS as JAX-RS / CXF
    participant Dispatcher as DefaultDispatcher
    participant BehaviourCall
    participant DAO as RdbmsDAOImpl
    participant DB as Database

    Client->>JAX-RS: HTTP Request
    JAX-RS->>Dispatcher: dispatch(operation, payload)
    Dispatcher->>Dispatcher: Authenticate & Authorize
    Dispatcher->>BehaviourCall: execute(payload)
    BehaviourCall->>DAO: CRUD operation
    DAO->>DB: SQL via StatementExecutor
    DB-->>DAO: ResultSet
    DAO-->>BehaviourCall: Payload
    BehaviourCall-->>Dispatcher: Response payload
    Dispatcher-->>JAX-RS: Serialized response
    JAX-RS-->>Client: HTTP Response
```

## Technology Stack

| Category | Technology | Version |
|----------|------------|---------|
| JVM | Java | 21 (LTS) |
| Build | Maven | 3.8.3+ |
| Code Generation | Lombok | 1.18.38 |
| DI (primary) | Google Guice | 5.1.0 |
| DI (alternative) | Spring Boot | 3.5.0 |
| REST | Apache CXF | 3.6.2 |
| Servlet Container | Jetty | 10.0.24 |
| Production DB | PostgreSQL | 42.7.7 (driver) |
| Dev/Test DB | HSQLDB | 2.6.1 |
| Connection Pool | HikariCP | 5.0.0 |
| Schema Migration | Liquibase | 4.9.1 |
| JSON | Jackson | 2.17.2 |
| Auth | Keycloak | 17.0.1 |
| Parser | ANTLR | 4.5.1-1 |
| Testing | JUnit 5 | 5.13.4 |
| Mocking | Mockito | 5.17.0 |
| Integration Tests | TestContainers | 1.21.1 |

## Build

```bash
# Full build with tests
mvn clean install

# Skip tests
mvn clean install -DskipTests

# Build parent POM only
mvn clean install -DskipModules=true

# Single module
mvn clean install -pl judo-runtime-core-dao-rdbms

# Run specific test
mvn clean test -pl judo-runtime-core-jackson -Dtest=LocalDateTimeSerializerTest
```

## Quick Start

1. **Clone and build:**
   ```bash
   git clone https://github.com/BlackBeltTechnology/judo-runtime-core.git
   cd judo-runtime-core
   ./mvnw clean install
   ```

2. **Choose your DI framework:**
   - Guice → `judo-runtime-core-guice-*` modules
   - Spring → `judo-runtime-core-spring-*` modules

3. **Choose your database:**
   - Development/testing → `*-hsqldb` modules (in-memory)
   - Production → `*-postgresql` modules

## Context

This project is a building block of the [judo-community](https://github.com/BlackBeltTechnology/judo-community) aggregator project. See the corresponding documentation for how this module fits into the broader JUDO ecosystem.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on submitting issues and pull requests.

## License

[Eclipse Public License - v 2.0](https://www.eclipse.org/legal/epl-2.0/)
