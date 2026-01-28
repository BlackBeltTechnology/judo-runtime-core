# Installing JUDO Security Keycloak CXF

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-security-keycloak-cxf
- **Version**: ${project.version}
- **License**: EPL-2.0

## Prerequisites

- Java 21+
- Maven 3.8.3+ (for dependency resolution)
- Running Keycloak instance (for runtime usage)
- Apache CXF runtime

## Installation

### Option 1: Extract from Maven Cache

If you have already built the project or have the artifact in your local Maven cache:

```bash
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-security-keycloak-cxf/${project.version}/judo-runtime-core-security-keycloak-cxf-${project.version}.jar
unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

### Option 2: Using Maven Dependency Plugin

Download the artifact and extract documentation:

```bash
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-security-keycloak-cxf:${project.version} -DoutputDirectory=.
unzip -o judo-runtime-core-security-keycloak-cxf-${project.version}.jar "claude/*" "agent-docs/*" -d .
```

### Option 3: One-liner

```bash
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-security-keycloak-cxf/${project.version}/judo-runtime-core-security-keycloak-cxf-${project.version}.jar && unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

## Maven Dependency

Add to your project's `pom.xml`:

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-security-keycloak-cxf</artifactId>
    <version>${project.version}</version>
</dependency>
```

## Available Documentation

After extraction, the following documentation is available in `agent-docs/`:

| File | Description |
|------|-------------|
| `README.md` | Module overview, authentication flow, and usage examples |

## Key Components

- **KeycloakLoginInterceptor** - CXF phase interceptor for JWT token validation
- **KeycloakSecurityContext** - Security context wrapper for authenticated principals

## Verification

Verify the installation by checking that the JAR contains the expected resources:

```bash
jar tf ~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-security-keycloak-cxf/${project.version}/judo-runtime-core-security-keycloak-cxf-${project.version}.jar | grep -E "(claude|agent-docs)"
```

Expected output:
```
claude/
claude/INSTALL.md
agent-docs/
agent-docs/README.md
```

## Related Modules

- `judo-runtime-core-security` - Core security interfaces (required)
- `judo-runtime-core-security-keycloak` - Keycloak connector (required)
- `judo-runtime-core-guice-keycloak` - Guice bindings for Keycloak integration
- `judo-runtime-core-jaxrs-cxf` - CXF JAX-RS integration
