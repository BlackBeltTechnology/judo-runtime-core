# Installing JUDO TestKit Agent Docs

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-guice-testkit
- **Version**: ${project.version}

## Prerequisites

- Claude Code CLI installed
- Maven project with this dependency
- Docker (for TestContainers)

## Installation

### Option 1: Extract from Maven cache

```bash
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-guice-testkit/${project.version}/judo-runtime-core-guice-testkit-${project.version}.jar
unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

### Option 2: Using Maven dependency plugin

```bash
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-guice-testkit:${project.version} -DoutputDirectory=./tmp
unzip -o ./tmp/judo-runtime-core-guice-testkit-${project.version}.jar "claude/*" "agent-docs/*" -d .
rm -rf ./tmp
```

### Option 3: One-liner

```bash
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-guice-testkit/${project.version}/judo-runtime-core-guice-testkit-${project.version}.jar && unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

## Available Documentation

After installation, documentation is available in `agent-docs/`:

| File | Content |
|------|---------|
| `agent-docs/README.md` | Overview and quick patterns |
| `agent-docs/interceptor-testing.md` | Deep dive on interceptor testing patterns |
| `agent-docs/api-reference.md` | Key classes and methods reference |
| `agent-docs/troubleshooting.md` | Common errors and solutions |
| `agent-docs/TEST-CONFIGURATION.md` | Test setup, dependencies, build order |

## Verification

After installation:

```bash
cat claude/marketplace.json
ls agent-docs/
```
