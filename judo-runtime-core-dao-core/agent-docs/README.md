# JUDO Runtime Core :: Core Data Access Objects

Core DAO interfaces, statements, and payload processors for data access operations.

## Overview

This module provides the foundational abstractions for data access in JUDO applications. It defines statement types for CRUD operations, payload processors for transforming data, and instance collection utilities.

## Maven Coordinates

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-dao-core</artifactId>
    <version>${judo-runtime-core-version}</version>
</dependency>
```

**Packaging:** OSGi bundle

## Key Components

### Statements (`statements/` package)

Define data operations to be executed:

| Statement | Purpose |
|-----------|---------|
| `InsertStatement` | Insert new entity instance |
| `UpdateStatement` | Update existing instance |
| `DeleteStatement` | Delete instance |
| `AddReferenceStatement` | Add reference between instances |
| `RemoveReferenceStatement` | Remove reference |
| `ReferenceStatement` | Base for reference operations |
| `ValidationStatement` | Base for validation checks |
| `InstanceExistsValidationStatement` | Check instance existence |
| `CheckUniqueAttributeStatement` | Validate attribute uniqueness |

### Payload Processors (`processors/` package)

Transform payloads into statements:

| Processor | Purpose |
|-----------|---------|
| `PayloadDaoProcessor` | Base processor interface |
| `InsertPayloadDaoProcessor` | Process insert payloads |
| `UpdatePayloadDaoProcessor` | Process update payloads |
| `DeletePayloadDaoProcessor` | Process delete payloads |
| `AddReferencePayloadDaoProcessor` | Process add reference |
| `RemoveReferencePayloadDaoProcessor` | Process remove reference |

### Instance Collectors (`collectors/` package)

Collect and manage instance data:

| Class | Purpose |
|-------|---------|
| `InstanceCollector` | Interface for collecting instances |
| `EmptyMapInstanceCollector` | No-op implementation |
| `InstanceGraph` | Graph structure for related instances |
| `InstanceReference` | Reference between instances |

### Value Objects (`values/` package)

Data containers:

| Class | Purpose |
|-------|---------|
| `InstanceValue` | Entity instance with attributes |
| `AttributeValue` | Single attribute value |
| `ReferenceValue` | Reference to another instance |
| `Metadata` | Instance metadata (type, version) |

## Usage Example

```java
// Create an insert statement
InsertStatement insertStmt = InsertStatement.builder()
    .instance(instanceValue)
    .type(entityType)
    .build();

// Process payload into statements
InsertPayloadDaoProcessor processor = new InsertPayloadDaoProcessor(
    asmModel, identifierProvider, coercer
);
Collection<Statement> statements = processor.process(payload, entityType);

// Collect instances
InstanceCollector collector = new RdbmsInstanceCollector(...);
InstanceGraph graph = collector.collectGraph(identifiers, entityType);
```

## Statement Execution Flow

1. **Payload received** - Transfer object data from client
2. **Processor converts** - Payload to Statement objects
3. **Statements collected** - Into execution batch
4. **Executor runs** - Statements against database
5. **Results mapped** - Back to transfer objects

## Related Modules

- `judo-runtime-core-dao-rdbms` - RDBMS implementation of DAO
- `judo-runtime-core-query` - Query building
- `judo-dao-api` - DAO API interfaces
- `judo-tatami-core` - Transformation services
