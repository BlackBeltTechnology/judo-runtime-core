---
name: judo-runtime:access-control
description: Comprehensive guide to configuring access control rules in JUDO Runtime. Use when you need to understand actor-based authorization, configure public vs authenticated access, set up exposedBy annotations, or implement custom authentication interceptors.
metadata:
  author: BlackBelt Technology
  version: "${project.version}"
---

# JUDO Access Control Configuration

Complete guide to configuring access control rules in JUDO Runtime Core.

## Overview

JUDO Access Control provides a declarative authorization system based on:
- Actor types (public vs authenticated)
- Operation exposure annotations (`exposedBy`)
- Realm-based multi-tenancy
- CRUD permission flags (create, update, delete)
- Custom authentication interceptors

## Architecture

```mermaid
graph TB
    subgraph "Request Flow"
        A[HTTP Request]
        B[Principal Extraction]
        C[AccessManager]
    end
    
    subgraph "Authorization Checks"
        D{Is Operation Exposed?}
        E{Is Actor Public?}
        F{Has Valid Token?}
        G[BehaviourAuthorizers]
    end
    
    subgraph "Outcomes"
        H[Allow Access]
        I[AuthenticationRequiredException]
        J[AccessDeniedException]
    end
    
    A --> B
    B --> C
    C --> D
    
    D -->|Yes| E
    D -->|No| F
    
    E -->|Public Actor| H
    E -->|Authenticated Actor| F
    
    F -->|Valid Token| G
    F -->|No Token| I
    F -->|Invalid Token| I
    
    G -->|Authorized| H
    G -->|Denied| J
```

## Actor Types

### Public Actors

Actors without a `realm` annotation are considered public and do not require authentication:

```
// ASM Model annotation
@actorType
@realm("")  // Empty or missing realm = public actor
entity PublicUser {
    // ...
}
```

### Authenticated Actors

Actors with a `realm` annotation require valid authentication tokens:

```
// ASM Model annotation  
@actorType
@realm("my-realm")  // Non-empty realm = authenticated actor
entity AdminUser {
    // ...
}
```

## Operation Exposure

Operations must be explicitly exposed to actors using the `exposedBy` annotation:

```mermaid
graph LR
    subgraph "Operations"
        A[listProducts]
        B[createOrder]
        C[deleteOrder]
    end
    
    subgraph "Actors"
        D[PublicUser]
        E[Customer]
        F[Admin]
    end
    
    A -->|exposedBy| D
    A -->|exposedBy| E
    A -->|exposedBy| F
    
    B -->|exposedBy| E
    B -->|exposedBy| F
    
    C -->|exposedBy| F
```

### Exposure Rules

| Scenario | Annotation | Result |
|----------|------------|--------|
| Public operation | `@exposedBy(PublicUser)` | Anyone can access |
| Customer-only | `@exposedBy(Customer)` | Requires Customer token |
| Multi-actor | `@exposedBy(Customer, Admin)` | Either actor can access |
| Not exposed | No `exposedBy` | Access denied |

## DefaultAccessManager

The `DefaultAccessManager` is the core authorization component:

```java
@Builder
public DefaultAccessManager(
    @NonNull AsmModel asmModel, 
    AuthenticationInterceptorProvider authenticationInterceptorProvider
)
```

### Authorization Flow

```mermaid
sequenceDiagram
    participant Dispatcher
    participant AccessManager as DefaultAccessManager
    participant Authorizer as BehaviourAuthorizer
    participant Interceptor as AuthenticationInterceptor
    
    Dispatcher->>AccessManager: authorizeOperation(operation, signedId, exchange)
    
    AccessManager->>AccessManager: Extract principal from exchange
    AccessManager->>AccessManager: Get actor FQN from principal
    
    alt Principal Operation
        AccessManager->>AccessManager: Check if principal exists
        alt No Principal
            AccessManager-->>Dispatcher: AuthenticationRequiredException
        end
    end
    
    AccessManager->>AccessManager: Check exposedBy annotation
    
    alt Not Exposed to Actor
        alt Has Token
            AccessManager-->>Dispatcher: AccessDeniedException
        else No Token
            AccessManager-->>Dispatcher: AuthenticationRequiredException
        end
    end
    
    alt Has SignedIdentifier
        AccessManager->>AccessManager: Check signedId.producedBy exposedBy
        alt Not Exposed
            AccessManager-->>Dispatcher: AccessDeniedException
        end
    end
    
    opt AuthenticationInterceptorProvider exists
        AccessManager->>Interceptor: isSuitableForOperation(...)
        AccessManager->>Interceptor: success(...)
    end
    
    loop Each suitable authorizer
        AccessManager->>Authorizer: isSuitableForOperation(operation)
        alt Suitable
            AccessManager->>Authorizer: authorize(actorFqName, publicActors, signedId, operation)
        end
    end
    
    AccessManager-->>Dispatcher: Authorization complete
```

## Configuration Examples

### Exposing Operations to Multiple Actors

```java
// In ASM model, operations are annotated with exposedBy
// The annotation value is the fully qualified name of the actor type

@exposedBy("Model.PublicActor")
@exposedBy("Model.AuthenticatedActor")
operation listItems() returns Item[];
```

### Detecting Public Actors

```java
// DefaultAccessManager identifies public actors during initialization
publicActors.addAll(asmUtils.all(EClass.class)
    .filter(c -> AsmUtils.isActorType(c) &&
        AsmUtils.getExtensionAnnotationByName(c, "realm", false)
            .map(a -> a.getDetails().get("value") == null || 
                      a.getDetails().get("value").isEmpty())
            .orElse(true))
    .map(AsmUtils::getClassifierFQName)
    .collect(Collectors.toSet()));
```

## Custom Authentication Interceptors

Implement `AuthenticationInterceptor` to add custom authorization logic:

```java
public class CustomAuthInterceptor implements AuthenticationInterceptor {
    
    @Override
    public String getName() {
        return "custom-auth";
    }
    
    @Override
    public boolean isSuitableForOperation(
            EOperation operation,
            String claim,
            String realm,
            String client,
            Map<String, Object> attributes) {
        // Return true if this interceptor should handle the operation
        return AsmUtils.getOperationFQName(operation).startsWith("Model.SecureService");
    }
    
    @Override
    public void success(
            EOperation operation,
            SignedIdentifier signedIdentifier,
            Map<String, Object> exchange,
            String claim,
            String realm,
            String client,
            Map<String, Object> attributes) {
        // Called after successful authorization
        // Add custom logic like audit logging, rate limiting, etc.
        log.info("User {} accessed {}", claim, AsmUtils.getOperationFQName(operation));
    }
}
```

### Registering Interceptors

```java
public class MyAuthInterceptorProvider implements AuthenticationInterceptorProvider {
    
    private final Collection<AuthenticationInterceptor> interceptors;
    
    public MyAuthInterceptorProvider() {
        interceptors = Arrays.asList(
            new CustomAuthInterceptor(),
            new AuditInterceptor()
        );
    }
    
    @Override
    public Collection<AuthenticationInterceptor> getAuthenticationInterceptors() {
        return interceptors;
    }
}
```

## Error Codes

| Code | Exception | Description |
|------|-----------|-------------|
| `INVALID_TOKEN` | `AuthenticationRequiredException` | Token is invalid or missing for principal operation |
| `ACCESS_DENIED` | `AccessDeniedException` | Operation not exposed to actor |
| `AUTHENTICATION_REQUIRED` | `AuthenticationRequiredException` | Token required for non-public operation |
| `ACCESS_DENIED_FOR_INSTANCE_OF_BOUND_OPERATION` | `AccessDeniedException` | No permission to access bound operation's instance |

## Access Control Matrix

```mermaid
graph TB
    subgraph "Operation Types"
        A[Metadata Operations]
        B[Public Operations]
        C[Authenticated Operations]
        D[Bound Operations]
    end
    
    subgraph "Access Rules"
        E[Always Allowed]
        F[Allowed if exposedBy matches]
        G[Requires valid token + exposedBy]
        H[Check signedIdentifier.producedBy]
    end
    
    A --> E
    B --> F
    C --> G
    D --> H
```

## Special Operation Types

### Metadata Operations

Operations with `GET_METADATA` behaviour are always accessible:

```java
// These bypass exposure checks
final boolean metadataOperation = AsmUtils.OperationBehaviour.GET_METADATA
    .equals(AsmUtils.getBehaviour(operation).orElse(null));
```

### Principal Operations

Operations with `GET_PRINCIPAL` behaviour require a valid principal:

```java
// Returns 401 if no principal
if (principal == null && principalOperation) {
    throw new AuthenticationRequiredException(ValidationResult.builder()
        .code("INVALID_TOKEN")
        .level(ValidationResult.Level.ERROR)
        .build());
}
```

## Debugging Access Control

### Enable Debug Logging

```xml
<logger name="hu.blackbelt.judo.runtime.core.accessmanager" level="DEBUG"/>
```

### Common Log Messages

| Message | Meaning |
|---------|---------|
| `Authorizing {actor} on {operation}` | Authorization check starting |
| `Operation failed, operation is not exposed to the given actor` | exposedBy mismatch |
| `Operation failed, authentication token is required` | Missing token for non-public operation |
| `Principal has no permission to access instance of bound operation` | SignedIdentifier check failed |

## See Also

- `judo-runtime-core-accessmanager-api` - API interfaces
- `judo-runtime-core-security` - Authentication framework
- `judo-runtime-core-dispatcher` - Operation dispatching
- `/judo-runtime:permission-checking` - CRUD permission details
