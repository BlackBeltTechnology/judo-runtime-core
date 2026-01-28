# Installing JUDO Access Manager Skills

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-accessmanager
- **Version**: ${project.version}

## Prerequisites

- Claude Code CLI installed
- Maven project with this dependency

## Installation

### Option 1: Extract from Maven cache

```bash
# Find the JAR
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-accessmanager/${project.version}/judo-runtime-core-accessmanager-${project.version}.jar

# Extract to current project
unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

### Option 2: Using Maven dependency plugin

```bash
# Copy JAR to target
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-accessmanager:${project.version} -DoutputDirectory=./tmp

# Extract
unzip -o ./tmp/judo-runtime-core-accessmanager-${project.version}.jar "claude/*" "agent-docs/*" -d .
rm -rf ./tmp
```

### Option 3: One-liner

```bash
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-accessmanager:${project.version} -DoutputDirectory=/tmp/judo-skill && unzip -o /tmp/judo-skill/*.jar "claude/*" "agent-docs/*" -d . && rm -rf /tmp/judo-skill
```

## Available Skills

| Skill | Command | Description |
|-------|---------|-------------|
| Access Control | `/judo-runtime:access-control` | Configure access control rules for actors and operations |
| Permission Checking | `/judo-runtime:permission-checking` | Understand the permission checking flow for CRUD operations |

## Key Components

| Class | Description |
|-------|-------------|
| `AccessManager` | Core interface for authorizing operation calls |
| `DefaultAccessManager` | Default implementation with behaviour-based authorization |
| `BehaviourAuthorizer` | Base class for operation-specific authorizers |
| `SignedIdentifier` | Identifies entities with producer and version information |
| `AuthenticationInterceptor` | Hook for custom authentication/authorization logic |

## Behaviour Authorizers

| Authorizer | Operations | CRUD Flags |
|------------|------------|------------|
| `CreateInstanceAuthorizer` | CREATE_INSTANCE, VALIDATE_CREATE | CREATE |
| `UpdateInstanceAuthorizer` | UPDATE_INSTANCE, VALIDATE_UPDATE | UPDATE |
| `DeleteInstanceAuthorizer` | DELETE_INSTANCE | DELETE |
| `ListAuthorizer` | LIST | exposedBy check |
| `SetReferenceAuthorizer` | SET_REFERENCE | UPDATE |
| `UnsetReferenceAuthorizer` | UNSET_REFERENCE | UPDATE |
| `AddReferenceAuthorizer` | ADD_REFERENCE | UPDATE |
| `RemoveReferenceAuthorizer` | REMOVE_REFERENCE | UPDATE |
| `GetReferenceRangeAuthorizer` | GET_REFERENCE_RANGE | CREATE or UPDATE |
| `GetInputRangeAuthorizer` | GET_INPUT_RANGE | exposedBy check |
| `GetTemplateAuthorizer` | GET_TEMPLATE | owner check |
| `RefreshAuthorizer` | REFRESH | no check |

## Related Modules

- `judo-runtime-core-accessmanager-api` - Access manager API interfaces
- `judo-runtime-core-security` - Authentication and security
- `judo-runtime-core-dispatcher` - Operation dispatching
- `judo-runtime-core-guice` - Guice bindings for access manager

## Verification

After installation, verify skills are available:

```bash
ls -la claude/plugins/judo-accessmanager/
cat claude/marketplace.json
```
