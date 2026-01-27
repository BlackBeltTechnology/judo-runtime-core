# DAO RDBMS Architecture

## Component Overview

```mermaid
flowchart TB
    subgraph "DAO Layer"
        DAO["RdbmsDAOImpl"]
        ABS["AbstractRdbmsDAO<br/>(Template Methods)"]
    end
    
    subgraph "Resolution Layer"
        RES["RdbmsResolver<br/>(Metamodel Queries)"]
        TTS["TransformationTraceService"]
    end
    
    subgraph "Query Translation"
        RB["RdbmsBuilder<br/>(Query Construction)"]
        MF["MapperFactory"]
        
        subgraph "Mappers"
            AM["AttributeMapper"]
            FM["FunctionMapper"]
            VM["VariableMapper"]
            FIM["FilterMapper"]
        end
        
        subgraph "Join Processors"
            FJP["FilterJoinProcessor"]
            CJP["ContainerJoinProcessor"]
            SJP["SubSelectJoinProcessor"]
        end
    end
    
    subgraph "Statement Execution"
        SE["StatementExecutor"]
        
        subgraph "Executors"
            SSE["SelectStatementExecutor"]
            ISE["InsertStatementExecutor"]
            USE["UpdateStatementExecutor"]
            DSE["DeleteStatementExecutor"]
        end
    end
    
    subgraph "Type Mapping"
        PM["RdbmsParameterMapper"]
        DPM["DefaultRdbmsParameterMapper"]
        DI["Dialect"]
    end
    
    DAO --> ABS
    ABS --> RES
    ABS --> RB
    RB --> MF
    MF --> AM
    MF --> FM
    MF --> VM
    MF --> FIM
    RB --> FJP
    RB --> CJP
    RB --> SJP
    ABS --> SE
    SE --> SSE
    SE --> ISE
    SE --> USE
    SE --> DSE
    SSE --> PM
    PM --> DPM
    DPM --> DI
    RES --> TTS
```

## Query Translation Flow

```mermaid
sequenceDiagram
    participant Client
    participant DAO as RdbmsDAOImpl
    participant Resolver as RdbmsResolver
    participant Builder as RdbmsBuilder
    participant Mapper as MapperFactory
    participant Translator as Translator
    participant Executor as StatementExecutor
    
    Client->>DAO: query(spec)
    DAO->>Resolver: Resolve metamodel
    Resolver-->>DAO: RDBMS tables/fields
    DAO->>Builder: Build query
    Builder->>Mapper: Map elements
    Mapper-->>Builder: Mapped fields
    Builder->>Translator: Translate expressions
    Translator-->>Builder: SQL fragments
    Builder-->>DAO: SQL + parameters
    DAO->>Executor: Execute
    Executor-->>DAO: Results
    DAO-->>Client: Mapped payloads
```

## Statement Executor Hierarchy

```mermaid
classDiagram
    class StatementExecutor {
        <<abstract>>
        +execute()
    }
    
    class SelectStatementExecutor {
        +executeQuery()
        +mapResults()
    }
    
    class ModifyStatementExecutor {
        <<abstract>>
        +executeModify()
    }
    
    class InsertStatementExecutor {
        +executeInsert()
    }
    
    class UpdateStatementExecutor {
        +executeUpdate()
    }
    
    class DeleteStatementExecutor {
        +executeDelete()
    }
    
    class AddReferenceStatementExecutor {
        +addReference()
    }
    
    class RemoveReferenceStatementExecutor {
        +removeReference()
    }
    
    StatementExecutor <|-- SelectStatementExecutor
    StatementExecutor <|-- ModifyStatementExecutor
    ModifyStatementExecutor <|-- InsertStatementExecutor
    ModifyStatementExecutor <|-- UpdateStatementExecutor
    ModifyStatementExecutor <|-- DeleteStatementExecutor
    StatementExecutor <|-- AddReferenceStatementExecutor
    StatementExecutor <|-- RemoveReferenceStatementExecutor
```

## Type Mapping

```mermaid
flowchart LR
    subgraph "Java Types"
        JT1["String"]
        JT2["Integer"]
        JT3["BigDecimal"]
        JT4["LocalDate"]
        JT5["Boolean"]
    end
    
    subgraph "RdbmsParameterMapper"
        PM["createParameter()"]
        ST["getSqlType()"]
    end
    
    subgraph "SQL Types"
        ST1["VARCHAR"]
        ST2["INTEGER"]
        ST3["DECIMAL"]
        ST4["DATE"]
        ST5["BOOLEAN"]
    end
    
    JT1 --> PM
    JT2 --> PM
    JT3 --> PM
    JT4 --> PM
    JT5 --> PM
    PM --> ST
    ST --> ST1
    ST --> ST2
    ST --> ST3
    ST --> ST4
    ST --> ST5
```

## Caching Strategy

The `SelectStatementExecutor` uses a query meta cache:

```mermaid
flowchart TB
    Q["Query Request"]
    C{"Cache Hit?"}
    CH["Return Cached Meta"]
    CM["Compute Meta"]
    SC["Store in Cache"]
    E["Execute Query"]
    
    Q --> C
    C -->|Yes| CH
    C -->|No| CM
    CM --> SC
    SC --> E
    CH --> E
```
