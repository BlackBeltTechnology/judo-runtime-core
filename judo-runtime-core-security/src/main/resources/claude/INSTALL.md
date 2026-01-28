# Installing JUDO Security Skills

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-security
- **Version**: ${project.version}

## Prerequisites

- Claude Code CLI installed
- Maven project with this dependency

## Installation

### Option 1: Extract from Maven cache

```bash
# Find the JAR
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-security/${project.version}/judo-runtime-core-security-${project.version}.jar

# Extract to current project
unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

### Option 2: Using Maven dependency plugin

```bash
# Copy JAR to target
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-security:${project.version} -DoutputDirectory=./tmp

# Extract
unzip -o ./tmp/judo-runtime-core-security-${project.version}.jar "claude/*" "agent-docs/*" -d .
rm -rf ./tmp
```

### Option 3: One-liner

```bash
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-security:${project.version} -DoutputDirectory=/tmp/judo-skill && unzip -o /tmp/judo-skill/*.jar "claude/*" "agent-docs/*" -d . && rm -rf /tmp/judo-skill
```

## Available Skills

| Skill | Command | Description |
|-------|---------|-------------|
| Authentication Flow | `/judo-runtime:authentication-flow` | Understand the complete authentication pipeline |
| Custom Auth | `/judo-runtime:custom-auth` | Implement custom authentication providers |

## Key Components

| Interface | Description |
|-----------|-------------|
| `OpenIdConfigurationProvider` | Provides OpenID Connect configuration for actor types |
| `RealmExtractor` | Extracts actor type (realm) from HTTP requests |
| `UserManager` | CRUD operations for user management in identity providers |
| `PasswordPolicy` | Defines password generation/validation rules |

## Related Modules

- `judo-runtime-core-security-keycloak` - Keycloak integration
- `judo-runtime-core-security-keycloak-cxf` - Keycloak + CXF interceptors
- `judo-runtime-core-guice-keycloak` - Guice bindings for Keycloak

## Verification

After installation, verify skills are available:

```bash
ls -la claude/plugins/judo-security/
cat claude/marketplace.json
```
