---
name: dispatcher-architecture
description: Understanding JUDO Dispatcher internals for advanced customization. Use when learning about operation routing, behaviour handlers, context management, variable providers, or transaction boundaries.
metadata:
  author: BlackBelt Technology
  version: "${project.version}"
---

# Dispatcher Architecture

Understanding the JUDO Dispatcher internals for advanced customization and debugging.

## Overview

The Dispatcher is the central routing component that handles all operation calls in a JUDO application. It routes requests to the appropriate handler (behaviour, script, or SDK implementation) and manages interceptors, transactions, and context.

## Architecture Diagram

```mermaid
flowchart TB
    subgraph "JUDO Application"
        REST["JAX-RS/CXF<br/>REST Endpoint"]
        
        subgraph "Dispatcher Module"
            DISP["DefaultDispatcher"]
            RC["RequestConverter"]
            RESP["ResponseConverter"]
            
            subgraph "Handlers"
                BH["Behaviour Handlers<br/>(CRUD)"]
                SH["Script Handlers<br/>(Model Scripts)"]
                SDK["SDK Handlers<br/>(Java Impl)"]
            end
            
            subgraph "Interceptors"
                INT["OperationCallInterceptor"]
                INTP["InterceptorProvider"]
            end
            
            subgraph "Context"
                TC["ThreadContext"]
                VR["VariableResolverManager"]
                AR["ActorResolver"]
            end
        end
        
        DAO["DAO Layer"]
    end
    
    REST --> RC
    RC --> DISP
    DISP --> BH
    DISP --> SH
    DISP --> SDK
    DISP --> INT
    BH --> DAO
    SH --> DAO
    SDK --> DAO
    DISP --> RESP
    RESP --> REST
    
    TC --> DISP
    VR --> DISP
    AR --> DISP
```

## Request Flow

```mermaid
sequenceDiagram
    participant Client
    participant REST as JAX-RS Endpoint
    participant RC as RequestConverter
    participant Disp as Dispatcher
    participant Int as Interceptors
    participant Handler as Operation Handler
    participant DAO
    participant Resp as ResponseConverter
    
    Client->>REST: HTTP Request
    REST->>RC: Convert to Payload
    RC->>Disp: dispatch(operation, payload)
    
    Disp->>Disp: Resolve actor/context
    Disp->>Int: preCall() for all interceptors
    
    alt Behaviour (CRUD)
        Disp->>Handler: BehaviourCall.execute()
    else Script
        Disp->>Handler: ScriptCall.execute()
    else SDK
        Disp->>Handler: SDKCall.execute()
    end
    
    Handler->>DAO: Data access
    DAO-->>Handler: Result
    Handler-->>Disp: Operation result
    
    Disp->>Int: postCall() for all interceptors
    Disp->>Resp: Convert result
    Resp-->>REST: Response payload
    REST-->>Client: HTTP Response
```

## Key Components

### DefaultDispatcher

The main dispatcher implementation that:
- Routes operations to appropriate handlers
- Manages transaction boundaries
- Invokes interceptors in order
- Handles error translation

**Location**: `hu.blackbelt.judo.runtime.core.dispatcher.DefaultDispatcher`

### Behaviour Handlers

Built-in CRUD operation handlers:

| Handler | Operation Type |
|---------|---------------|
| `CreateInstanceCall` | Create new entity |
| `UpdateInstanceCall` | Update existing entity |
| `DeleteInstanceCall` | Delete entity |
| `ListCall` | Query/list entities |
| `RefreshCall` | Reload entity from DB |
| `GetTemplateCall` | Get default values |
| `ValidateCreateCall` | Validate before create |
| `ValidateUpdateCall` | Validate before update |

**Location**: `hu.blackbelt.judo.runtime.core.dispatcher.behaviours.*`

### Context Management

```mermaid
graph LR
    subgraph "Thread Context"
        TC["ThreadContext"]
        TC --> ACTOR["Current Actor"]
        TC --> PARAMS["Request Parameters"]
        TC --> TX["Transaction"]
    end
    
    subgraph "Variable Resolution"
        VRM["VariableResolverManager"]
        VRM --> DATE["CurrentDateProvider"]
        VRM --> TIME["CurrentTimeProvider"]
        VRM --> TS["CurrentTimestampProvider"]
        VRM --> PRINC["PrincipalVariableProvider"]
        VRM --> ENV["EnvironmentVariableProvider"]
    end
```

### Variable Providers

Built-in providers for expression context:

| Provider | Variable | Description |
|----------|----------|-------------|
| `CurrentDateProvider` | `CURRENT_DATE` | Today's date |
| `CurrentTimeProvider` | `CURRENT_TIME` | Current time |
| `CurrentTimestampProvider` | `CURRENT_TIMESTAMP` | Current date+time |
| `PrincipalVariableProvider` | `PRINCIPAL` | Authenticated user |
| `ActorVariableProvider` | `ACTOR` | Current actor context |
| `EnvironmentVariableProvider` | `ENV` | Environment variables |

## Extension Points

### Adding Custom Variable Providers

```java
public class MyVariableProvider implements EnvironmentVariableProvider {
    @Override
    public Optional<Object> get(String name) {
        if ("MY_VAR".equals(name)) {
            return Optional.of(computeValue());
        }
        return Optional.empty();
    }
}
```

### Custom Dispatcher Functions

Implement `DispatcherFunctionProvider` to add custom functions:

```java
public class MyFunctionProvider implements DispatcherFunctionProvider {
    @Override
    public Map<String, Function<Object[], Object>> getFunctions() {
        return Map.of(
            "myFunction", args -> processArgs(args)
        );
    }
}
```

## Transaction Management

```mermaid
stateDiagram-v2
    [*] --> Begin: Start operation
    Begin --> Execute: Transaction opened
    Execute --> PreCommit: Success
    Execute --> Rollback: Exception
    PreCommit --> Commit: Interceptors OK
    PreCommit --> Rollback: Interceptor fails
    Commit --> [*]: Success
    Rollback --> [*]: Error response
```

The dispatcher manages transactions via Spring's `PlatformTransactionManager`:
- Transaction opened before operation
- Committed after successful execution + interceptors
- Rolled back on any exception

## Debugging Tips

1. **Enable dispatcher logging**:
   ```xml
   <logger name="hu.blackbelt.judo.runtime.core.dispatcher" level="DEBUG"/>
   ```

2. **Check interceptor order**: Interceptors run in registration order

3. **Thread context issues**: Use `ThreadContext` to access current context

4. **Variable resolution failures**: Check if provider is registered

## See Also

- `/judo-dispatcher:create-interceptor` - How to create interceptors
- `/judo-dispatcher:debug-operations` - Debugging operation issues
- `agent-docs/extension-points.md` - All extension interfaces
