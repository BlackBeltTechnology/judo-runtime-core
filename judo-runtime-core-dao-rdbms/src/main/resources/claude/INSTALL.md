# Installing JUDO DAO RDBMS Skills

## Module Information
- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-dao-rdbms
- **Version**: ${project.version}

## Installation

### Extract from Maven cache

```bash
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-dao-rdbms/${project.version}/judo-runtime-core-dao-rdbms-${project.version}.jar
unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

## Available Skills

| Skill | Command | Description |
|-------|---------|-------------|
| Custom Queries | `/judo-dao-rdbms:custom-queries` | Extend query translation with custom mappers |
| Query Debugging | `/judo-dao-rdbms:query-debugging` | Debug and trace SQL generation |
| Dialect Extension | `/judo-dao-rdbms:dialect-extension` | Add support for new database dialects |

## Key Extension Points

- `Dialect` - Database dialect abstraction
- `RdbmsParameterMapper` - Type mapping for SQL parameters
- `MapperFactory` - Query element mappers
- `Translator` - Expression translation functions
