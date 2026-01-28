# Installing JUDO DAO Core Skills

## Module Information
- **GroupId**: hu.blackbelt.judo.runtime
- **ArtifactId**: judo-runtime-core-dao-core
- **Version**: ${project.version}

## Installation

### Extract from Maven cache

```bash
JAR=~/.m2/repository/hu/blackbelt/judo/runtime/judo-runtime-core-dao-core/${project.version}/judo-runtime-core-dao-core-${project.version}.jar
unzip -o "$JAR" "claude/*" "agent-docs/*" -d .
```

## Available Skills

| Skill | Command | Description |
|-------|---------|-------------|
| DAO Patterns | `/judo-dao-core:dao-patterns` | Understand DAO interface patterns - statements, processors, and collectors |
| Entity Mapping | `/judo-dao-core:entity-mapping` | Entity to transfer object mapping with values and metadata |

## Key Components

### Processors
- `PayloadDaoProcessor` - Base processor with validation utilities
- `InsertPayloadDaoProcessor` - Insert statement generation
- `UpdatePayloadDaoProcessor` - Update statement generation with merge logic
- `DeletePayloadDaoProcessor` - Delete statement generation with cascade handling

### Statements
- `InsertStatement` - Entity creation with metadata
- `UpdateStatement` - Entity modification with optimistic locking
- `DeleteStatement` - Entity removal
- `AddReferenceStatement` / `RemoveReferenceStatement` - Reference management
- `ValidationStatement` - Existence validation

### Values
- `InstanceValue` - Entity instance representation with attributes
- `AttributeValue` - Single attribute value holder
- `ReferenceValue` - Reference relationship value
- `Metadata` - Audit metadata (userId, username, timestamp)

### Collectors
- `InstanceCollector` - Interface for collecting instance graphs
- `InstanceGraph` - Graph of containments, references, and back-references
- `InstanceReference` - Reference edge in instance graph
