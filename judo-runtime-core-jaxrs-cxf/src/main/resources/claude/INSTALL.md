# Installing JUDO JAX-RS CXF Skills

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-jaxrs-cxf
- **Version**: ${project.version}

## Prerequisites

- Claude Code CLI installed
- Maven project with this dependency

## Installation

### Option 1: Extract from Maven cache

```bash
# Find the JAR
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-jaxrs-cxf/${project.version}/judo-runtime-core-jaxrs-cxf-${project.version}.jar

# Extract to current project
unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

### Option 2: Using Maven dependency plugin

```bash
# Copy JAR to target
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-jaxrs-cxf:${project.version} -DoutputDirectory=./tmp

# Extract
unzip -o ./tmp/judo-runtime-core-jaxrs-cxf-${project.version}.jar "claude/*" "agent-docs/*" -d .
rm -rf ./tmp
```

### Option 3: One-liner

```bash
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-jaxrs-cxf:${project.version} -DoutputDirectory=/tmp/judo-skill && unzip -o /tmp/judo-skill/*.jar "claude/*" "agent-docs/*" -d . && rm -rf /tmp/judo-skill
```

## Available Skills

| Skill | Command | Description |
|-------|---------|-------------|
| CXF Integration | `/judo-runtime:cxf-integration` | Apache CXF integration for JAX-RS with interceptors, fault handling, and authorization |

## Available Documentation

| Document | Description |
|----------|-------------|
| `agent-docs/README.md` | Module overview |
| `agent-docs/interceptors.md` | CXF interceptor documentation |
| `agent-docs/providers.md` | JAX-RS provider documentation |

## Verification

After installation, verify skills are available:

```bash
ls -la claude/plugins/judo-jaxrs-cxf/
cat claude/marketplace.json
```
