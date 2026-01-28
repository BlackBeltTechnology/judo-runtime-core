# Installing JUDO Query Skills

## Module Information
- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-query
- **Version**: ${project.version}

## Installation

### Extract from Maven cache

```bash
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-query/${project.version}/judo-runtime-core-query-${project.version}.jar
unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

## Available Skills

| Skill | Command | Description |
|-------|---------|-------------|
| Query Translation | `/judo-query:query-translation` | Understand query model to SQL translation |
| Query Optimization | `/judo-query:query-optimization` | Optimize query performance |

## Key Classes

- `QueryFactory` - Main entry point for logical query generation
- `JoinFactory` - Navigation path to JOIN conversion
- `FeatureFactory` - Expression to SQL feature conversion
- `Context` - Query building context with variable tracking
- `ExpressionToFeatureConverter` - Base class for expression converters
