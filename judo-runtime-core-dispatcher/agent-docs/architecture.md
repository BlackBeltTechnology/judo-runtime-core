# Dispatcher Architecture

## System Overview

```mermaid
flowchart TB
    subgraph "External"
        CLIENT[Client Application]
    end
    
    subgraph "API Layer"
        REST["JAX-RS/CXF<br/>REST Endpoints"]
    end
    
    subgraph "Dispatcher Module"
        RC["RequestConverter"]
        DISP["DefaultDispatcher"]
        RESP["ResponseConverter"]
        
        subgraph "Operation Handlers"
            BH["Behaviour Handlers"]
            SH["Script Handlers"]
            SDK["SDK Handlers"]
        end
        
        subgraph "Interceptor Chain"
            INT1["Interceptor 1"]
            INT2["Interceptor 2"]
            INTN["Interceptor N"]
        end
        
        subgraph "Context Services"
            AR["ActorResolver"]
            VRM["VariableResolverManager"]
            TC["ThreadContext"]
        end
    end
    
    subgraph "Data Layer"
        DAO["DAO"]
        DB[(Database)]
    end
    
    CLIENT --> REST
    REST --> RC --> DISP
    DISP --> INT1 --> INT2 --> INTN
    DISP --> BH & SH & SDK
    BH & SH & SDK --> DAO --> DB
    DISP --> RESP --> REST --> CLIENT
    AR & VRM & TC -.-> DISP
```

## Request Processing Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant R as REST Endpoint
    participant RC as RequestConverter
    participant D as Dispatcher
    participant I as Interceptors
    participant H as Handler
    participant DAO as DAO
    participant DB as Database
    
    C->>R: HTTP Request
    R->>RC: Raw request
    RC->>D: Payload + Operation
    
    Note over D: Begin Transaction
    
    D->>D: Resolve Actor
    D->>D: Setup Context
    
    loop For each Interceptor
        D->>I: preCall(operation, payload)
        I-->>D: Modified payload
    end
    
    alt Behaviour Operation
        D->>H: BehaviourCall.execute()
    else Script Operation
        D->>H: ScriptCall.execute()
    else SDK Operation
        D->>H: SDKCall.execute()
    end
    
    H->>DAO: Data operation
    DAO->>DB: SQL
    DB-->>DAO: Result
    DAO-->>H: Entities
    H-->>D: Result payload
    
    loop For each Interceptor (reverse)
        D->>I: postCall(operation, input, result)
        I-->>D: Modified result
    end
    
    Note over D: Commit Transaction
    
    D-->>R: Response payload
    R-->>C: HTTP Response
```

## Component Details

### DefaultDispatcher

The central orchestrator that:

```mermaid
graph LR
    subgraph "DefaultDispatcher Responsibilities"
        A[Route Operations] --> B[Manage Transactions]
        B --> C[Invoke Interceptors]
        C --> D[Handle Errors]
        D --> E[Convert Responses]
    end
```

Key methods:
- `dispatch(operation, payload)` - Main entry point
- `callOperation()` - Routes to appropriate handler
- `callInterceptors()` - Manages interceptor chain

### Operation Types

```mermaid
graph TB
    OP[Operation] --> BEH[Behaviour]
    OP --> SCR[Script]
    OP --> SDK[SDK]
    
    BEH --> CR[Create]
    BEH --> UP[Update]
    BEH --> DE[Delete]
    BEH --> LI[List]
    BEH --> RE[Refresh]
    BEH --> VA[Validate]
    
    SCR --> MS[Model Script]
    
    SDK --> JI[Java Implementation]
```

### Behaviour Handlers

| Handler | Purpose | Key Method |
|---------|---------|------------|
| `CreateInstanceCall` | Create new entity | `execute()` |
| `UpdateInstanceCall` | Update entity | `execute()` |
| `DeleteInstanceCall` | Delete entity | `execute()` |
| `ListCall` | Query entities | `execute()` |
| `RefreshCall` | Reload from DB | `execute()` |
| `GetTemplateCall` | Default values | `execute()` |
| `ValidateCreateCall` | Pre-create validation | `execute()` |
| `ValidateUpdateCall` | Pre-update validation | `execute()` |
| `AddReferenceCall` | Add to relation | `execute()` |
| `RemoveReferenceCall` | Remove from relation | `execute()` |
| `SetReferenceCall` | Set relation | `execute()` |
| `UnsetReferenceCall` | Clear relation | `execute()` |

### Interceptor Chain

```mermaid
stateDiagram-v2
    [*] --> PreCall1: Interceptor 1
    PreCall1 --> PreCall2: Interceptor 2
    PreCall2 --> PreCallN: Interceptor N
    PreCallN --> Execute: Handler
    Execute --> PostCallN: Interceptor N
    PostCallN --> PostCall2: Interceptor 2
    PostCall2 --> PostCall1: Interceptor 1
    PostCall1 --> [*]
    
    note right of Execute: Actual operation
    note left of PreCall1: preCall() methods
    note right of PostCall1: postCall() methods
```

### Context Management

```mermaid
graph TB
    subgraph "Thread Context"
        TC[ThreadContext]
        TC --> |stores| ACTOR[Current Actor]
        TC --> |stores| PARAMS[Request Params]
        TC --> |stores| TX[Transaction State]
        TC --> |stores| LOCALE[Locale]
    end
    
    subgraph "Variable Resolution"
        VRM[VariableResolverManager]
        VRM --> CDP[CurrentDateProvider]
        VRM --> CTP[CurrentTimeProvider]
        VRM --> CTSP[CurrentTimestampProvider]
        VRM --> PVP[PrincipalVariableProvider]
        VRM --> AVP[ActorVariableProvider]
        VRM --> EVP[EnvironmentVariableProvider]
        VRM --> RPVP[RequestParamsProvider]
    end
    
    subgraph "Actor Resolution"
        AR[ActorResolver]
        AR --> |resolves| KEYCLOAK[Keycloak Token]
        AR --> |resolves| CUSTOM[Custom Auth]
    end
```

## Transaction Management

```mermaid
stateDiagram-v2
    [*] --> Started: Begin Transaction
    Started --> Executing: Run Operation
    Executing --> PreCommit: Success
    Executing --> RollbackPending: Exception
    PreCommit --> Committed: All OK
    PreCommit --> RollbackPending: Interceptor Fails
    RollbackPending --> RolledBack: Rollback Complete
    Committed --> [*]
    RolledBack --> [*]
```

Transaction boundaries:
- Opened before first interceptor `preCall()`
- Committed after last interceptor `postCall()`
- Rolled back on any exception
- Supports `alwaysRollback` for testing

## Error Handling

```mermaid
flowchart TD
    ERR[Exception Thrown] --> TYPE{Exception Type}
    
    TYPE -->|ValidationException| VE[Return validation errors]
    TYPE -->|AccessDeniedException| AD[Return 403]
    TYPE -->|NotFoundException| NF[Return 404]
    TYPE -->|BusinessException| BE[Return business error]
    TYPE -->|Other| OT[Return 500 + log]
    
    VE & AD & NF & BE --> ROLLBACK[Rollback Transaction]
    OT --> ROLLBACK
    ROLLBACK --> RESPONSE[Error Response]
```

## Integration Points

```mermaid
graph LR
    subgraph "Dispatcher Integration"
        DISP[Dispatcher]
    end
    
    subgraph "Upstream"
        REST[JAX-RS/CXF]
        GUICE[Guice Module]
        SPRING[Spring Boot]
    end
    
    subgraph "Downstream"
        DAO[DAO Layer]
        VALID[Validator]
        EXPR[Expression Engine]
        ACCESS[Access Manager]
    end
    
    REST --> DISP
    GUICE --> DISP
    SPRING --> DISP
    
    DISP --> DAO
    DISP --> VALID
    DISP --> EXPR
    DISP --> ACCESS
```
