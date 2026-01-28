# Installing JUDO Guice Skills

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-guice
- **Version**: ${project.version}

## Prerequisites

- Claude Code CLI installed
- Maven project with this dependency

## Installation

### Option 1: Extract from Maven cache

```bash
# Find the JAR
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-guice/${project.version}/judo-runtime-core-guice-${project.version}.jar

# Extract to current project
unzip -o "$JAR" "claude/*" -d .
```

### Option 2: Using Maven dependency plugin

```bash
# Copy JAR to target
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-guice:${project.version} -DoutputDirectory=./tmp

# Extract
unzip -o ./tmp/judo-runtime-core-guice-${project.version}.jar "claude/*" -d .
rm -rf ./tmp
```

### Option 3: One-liner

```bash
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-guice:${project.version} -DoutputDirectory=/tmp/judo-skill && unzip -o /tmp/judo-skill/*.jar "claude/*" -d . && rm -rf /tmp/judo-skill
```

## Available Skills

| Skill | Command | Description |
|-------|---------|-------------|
| Module Setup | `/judo-runtime:module-setup` | Configure JudoDefaultModule and model loading |
| Dependency Injection | `/judo-runtime:dependency-injection` | Understand and extend DI bindings |

## Verification

After installation, verify skills are available:

```bash
ls -la claude/plugins/judo-guice/
cat claude/marketplace.json
```
