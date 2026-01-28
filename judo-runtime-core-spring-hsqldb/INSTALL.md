# JUDO Runtime Core Spring HSQLDB - Installation Guide

## Module Information

| Property | Value |
|----------|-------|
| **GroupId** | `hu.blackbelt.judo.runtime` |
| **ArtifactId** | `judo-runtime-core-spring-hsqldb` |
| **Version** | `1.0.6-SNAPSHOT` |
| **Packaging** | `jar` |

## Prerequisites

- Java 21 or higher
- Maven 3.8.3+ (for building from source)
- Spring Boot 3.5.0+ application

## Installation Options

### Option 1: Extract from Maven Local Cache

If you have built the project locally:

```bash
# Find the JAR in your local Maven repository
ls ~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-spring-hsqldb/1.0.6-SNAPSHOT/

# Copy the JAR to your desired location
cp ~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-spring-hsqldb/1.0.6-SNAPSHOT/judo-runtime-core-spring-hsqldb-1.0.6-SNAPSHOT.jar ./
```

### Option 2: Using Maven Dependency Plugin

Download the artifact directly using Maven:

```bash
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-spring-hsqldb:1.0.6-SNAPSHOT -DoutputDirectory=./lib
```

### Option 3: One-liner Download

Using Maven dependency:get plugin:

```bash
mvn dependency:get -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-spring-hsqldb:1.0.6-SNAPSHOT
```

### Option 4: Add as Maven Dependency

Add to your project's `pom.xml`:

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-spring-hsqldb</artifactId>
    <version>1.0.6-SNAPSHOT</version>
</dependency>
```

## Available Documentation

Additional module documentation is available in the `agent-docs/` directory within the JAR:

| Document | Description |
|----------|-------------|
| `agent-docs/README.md` | Module overview, usage, and configuration |

To extract documentation from the JAR:

```bash
# Extract agent-docs from the JAR
unzip -j judo-runtime-core-spring-hsqldb-1.0.6-SNAPSHOT.jar "agent-docs/*" -d ./docs
```

## Verification

Verify the installation by checking the JAR contents:

```bash
# List JAR contents
jar tf judo-runtime-core-spring-hsqldb-1.0.6-SNAPSHOT.jar

# Verify main configuration class exists
jar tf judo-runtime-core-spring-hsqldb-1.0.6-SNAPSHOT.jar | grep JudoHsqldbSpringConfiguration

# Check for agent-docs
jar tf judo-runtime-core-spring-hsqldb-1.0.6-SNAPSHOT.jar | grep agent-docs
```

## Quick Start

After installation, configure your Spring Boot application:

```properties
# application.properties
spring.datasource.url=jdbc:hsqldb:mem:testdb
spring.datasource.driver-class-name=org.hsqldb.jdbc.JDBCDriver
spring.datasource.username=sa
spring.datasource.password=
```

The HSQLDB configuration will be automatically activated when the datasource URL contains `hsqldb`.
