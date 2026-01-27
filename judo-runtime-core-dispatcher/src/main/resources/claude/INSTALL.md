# Installing JUDO Dispatcher Skills

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-dispatcher
- **Version**: ${project.version}

## Prerequisites

- Claude Code CLI installed
- Maven project with this dependency

## Installation

### Option 1: Extract from Maven cache

```bash
# Find the JAR
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-dispatcher/${project.version}/judo-runtime-core-dispatcher-${project.version}.jar

# Extract to current project
unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

### Option 2: Using Maven dependency plugin

```bash
# Copy JAR to target
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-dispatcher:${project.version} -DoutputDirectory=./tmp

# Extract
unzip -o ./tmp/judo-runtime-core-dispatcher-${project.version}.jar "claude/*" "agent-docs/*" -d .
rm -rf ./tmp
```

### Option 3: One-liner

```bash
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-dispatcher:${project.version} -DoutputDirectory=/tmp/judo-skill && unzip -o /tmp/judo-skill/*.jar "claude/*" "agent-docs/*" -d . && rm -rf /tmp/judo-skill
```

## Available Skills

| Skill | Command | Description |
|-------|---------|-------------|
| Create Interceptor | `/judo-dispatcher:create-interceptor` | Step-by-step guide to create operation interceptors |
| Architecture | `/judo-dispatcher:dispatcher-architecture` | Understand dispatcher internals |
| Debug Operations | `/judo-dispatcher:debug-operations` | Troubleshoot operation execution |

## Available Documentation

| Document | Description |
|----------|-------------|
| `agent-docs/README.md` | Module overview |
| `agent-docs/architecture.md` | Internal architecture and flow |
| `agent-docs/extension-points.md` | All extension interfaces |
| `agent-docs/examples/` | Code examples |

## Verification

After installation, verify skills are available:

```bash
ls -la claude/plugins/judo-dispatcher/
cat claude/marketplace.json
```
