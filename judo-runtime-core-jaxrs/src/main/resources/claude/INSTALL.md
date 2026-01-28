# Installing JUDO JAX-RS Skills

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-jaxrs
- **Version**: ${project.version}

## Prerequisites

- Claude Code CLI installed
- Maven project with this dependency

## Installation

### Option 1: Extract from Maven cache

```bash
# Find the JAR
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-jaxrs/${project.version}/judo-runtime-core-jaxrs-${project.version}.jar

# Extract to current project
unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

### Option 2: Using Maven dependency plugin

```bash
# Copy JAR to target
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-jaxrs:${project.version} -DoutputDirectory=./tmp

# Extract
unzip -o ./tmp/judo-runtime-core-jaxrs-${project.version}.jar "claude/*" "agent-docs/*" -d .
rm -rf ./tmp
```

### Option 3: One-liner

```bash
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-jaxrs:${project.version} -DoutputDirectory=/tmp/judo-skill && unzip -o /tmp/judo-skill/*.jar "claude/*" "agent-docs/*" -d . && rm -rf /tmp/judo-skill
```

## Available Skills

| Skill | Command | Description |
|-------|---------|-------------|
| REST Endpoints | `/judo-runtime:rest-endpoints` | Configure JAX-RS providers, exception mappers, and request filters |

## Key Components

| Class | Type | Description |
|-------|------|-------------|
| `PayloadMessageBodyWriter` | MessageBodyWriter | JSON serialization for Payload objects |
| `ClientExceptionMapper` | ExceptionMapper | Maps ClientException to HTTP responses |
| `RuntimeExceptionMapper` | ExceptionMapper | Maps RuntimeException and BusinessException to HTTP responses |
| `ISO8601DateParamHandler` | ParamConverterProvider | Handles ISO8601 date parameter parsing |
| `SetDefaultContentTypePreMatchContainerRequestFilter` | ContainerRequestFilter | Sets default Content-Type for requests |

## Related Modules

- `judo-runtime-core-jaxrs-cxf` - Apache CXF specific interceptors
- `judo-runtime-core-jaxrs-cxf-server` - CXF server bootstrap
- `judo-runtime-core-jackson` - Jackson JSON providers
- `judo-runtime-core-guice-cxf` - Guice + CXF integration

## Verification

After installation, verify skills are available:

```bash
ls -la claude/plugins/judo-jaxrs/
cat claude/marketplace.json
```
