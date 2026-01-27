# Installing JUDO Expression Skills

## Module Information
- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-expression
- **Version**: ${project.version}

## Installation

### Extract from Maven cache

```bash
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-expression/${project.version}/judo-runtime-core-expression-${project.version}.jar
unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

## Available Skills

| Skill | Command | Description |
|-------|---------|-------------|
| Expression Syntax | `/judo-expression:expression-syntax` | Understand expression metamodel and bindings |
| Custom Functions | `/judo-expression:custom-functions` | Add custom expression translators |

## Key Classes

- `TransferObjectTypeBindingsCollector` - Expression tree orchestrator
- `MappedTransferObjectTypeBindings` - Expression tree nodes
- `EntityTypeExpressions` - Entity-level expression cache
