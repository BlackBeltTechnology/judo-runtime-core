# Installing JUDO Access Manager API Skills

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-accessmanager-api
- **Version**: ${project.version}

## Prerequisites

- Claude Code CLI installed
- Maven project with this dependency

## Installation

### Option 1: Extract from Maven cache

```bash
# Find the JAR
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-accessmanager-api/${project.version}/judo-runtime-core-accessmanager-api-${project.version}.jar

# Extract to current project
unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

### Option 2: Using Maven dependency plugin

```bash
# Copy JAR to target
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-accessmanager-api:${project.version} -DoutputDirectory=./tmp

# Extract
unzip -o ./tmp/judo-runtime-core-accessmanager-api-${project.version}.jar "claude/*" "agent-docs/*" -d .
rm -rf ./tmp
```

### Option 3: One-liner

```bash
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-accessmanager-api:${project.version} -DoutputDirectory=/tmp/judo-skill && unzip -o /tmp/judo-skill/*.jar "claude/*" "agent-docs/*" -d . && rm -rf /tmp/judo-skill
```

## Available Skills

| Skill | Command | Description |
|-------|---------|-------------|
| Access API Overview | `/judo-runtime:access-api-overview` | Comprehensive guide to Access Manager API interfaces |

## Key Components

| Interface | Description |
|-----------|-------------|
| `AccessManager` | Core interface for authorizing operation calls |
| `AuthenticationInterceptor` | Interceptor for authentication/authorization lifecycle hooks |
| `AuthenticationInterceptorProvider` | Provider for authentication interceptors |
| `SignedIdentifier` | Identifies bound operations with entity metadata |

## Related Modules

- `judo-runtime-core-accessmanager` - Default implementation of AccessManager
- `judo-runtime-core-security` - Authentication and user management
- `judo-runtime-core-dispatcher` - Operation dispatching (uses AccessManager)

## Verification

After installation, verify skills are available:

```bash
ls -la claude/plugins/judo-accessmanager-api/
cat claude/marketplace.json
```
