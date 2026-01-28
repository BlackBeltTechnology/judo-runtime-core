# Installing JUDO DAO RDBMS HSQLDB Documentation

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-dao-rdbms-hsqldb
- **Version**: ${project.version}

## Prerequisites

- Claude Code CLI installed
- Maven project with this dependency

## Installation

### Option 1: Extract from Maven cache

```bash
# Find the JAR
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-dao-rdbms-hsqldb/${project.version}/judo-runtime-core-dao-rdbms-hsqldb-${project.version}.jar

# Extract to current project
unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

### Option 2: Using Maven dependency plugin

```bash
# Copy JAR to target
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-dao-rdbms-hsqldb:${project.version} -DoutputDirectory=./tmp

# Extract
unzip -o ./tmp/judo-runtime-core-dao-rdbms-hsqldb-${project.version}.jar "claude/*" "agent-docs/*" -d .
rm -rf ./tmp
```

### Option 3: One-liner

```bash
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-dao-rdbms-hsqldb:${project.version} -DoutputDirectory=/tmp/judo-skill && unzip -o /tmp/judo-skill/*.jar "claude/*" "agent-docs/*" -d . && rm -rf /tmp/judo-skill
```

## Available Documentation

| Document | Description |
|----------|-------------|
| `agent-docs/README.md` | Module overview, key components, and usage |

## Verification

After installation, verify documentation is available:

```bash
ls -la agent-docs/
cat agent-docs/README.md
```
