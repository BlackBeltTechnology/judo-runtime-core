# Installing JUDO CXF Server Agent Docs

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-jaxrs-cxf-server
- **Version**: ${project.version}

## Prerequisites

- Claude Code CLI installed
- Maven project with this dependency

## Installation

### Option 1: Extract from Maven cache

```bash
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-jaxrs-cxf-server/${project.version}/judo-runtime-core-jaxrs-cxf-server-${project.version}.jar
unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

### Option 2: Using Maven dependency plugin

```bash
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-jaxrs-cxf-server:${project.version} -DoutputDirectory=./tmp
unzip -o ./tmp/judo-runtime-core-jaxrs-cxf-server-${project.version}.jar "claude/*" "agent-docs/*" -d .
rm -rf ./tmp
```

### Option 3: One-liner

```bash
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-jaxrs-cxf-server/${project.version}/judo-runtime-core-jaxrs-cxf-server-${project.version}.jar && unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

## Available Documentation

After installation, documentation is available in `agent-docs/`:

| File | Content |
|------|---------|
| `agent-docs/README.md` | Module overview and component documentation |

## Verification

After installation:

```bash
cat claude/marketplace.json
ls agent-docs/
```
