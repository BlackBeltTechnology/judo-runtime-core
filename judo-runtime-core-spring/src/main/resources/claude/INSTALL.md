# Installing JUDO Spring Skills

## Module Information

- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-spring
- **Version**: ${project.version}

## Prerequisites

- Claude Code CLI installed
- Maven project with this dependency

## Installation

### Option 1: Extract from Maven cache

```bash
# Find the JAR
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-spring/${project.version}/judo-runtime-core-spring-${project.version}.jar

# Extract to current project
unzip -o "$JAR" "claude/*" -d .
```

### Option 2: Using Maven dependency plugin

```bash
# Copy JAR to target
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-spring:${project.version} -DoutputDirectory=./tmp

# Extract
unzip -o ./tmp/judo-runtime-core-spring-${project.version}.jar "claude/*" -d .
rm -rf ./tmp
```

### Option 3: One-liner

```bash
mvn dependency:copy -Dartifact=hu.blackbelt.judo.runtime:judo-runtime-core-spring:${project.version} -DoutputDirectory=/tmp/judo-skill && unzip -o /tmp/judo-skill/*.jar "claude/*" -d . && rm -rf /tmp/judo-skill
```

## Available Skills

| Skill | Command | Description |
|-------|---------|-------------|
| Autoconfiguration | `/judo-runtime:autoconfiguration` | Understand JUDO Spring Boot autoconfiguration |
| Spring Integration | `/judo-runtime:spring-integration` | Integrate JUDO with Spring applications |

## Verification

After installation, verify skills are available:

```bash
ls -la claude/plugins/judo-spring/
cat claude/marketplace.json
```
