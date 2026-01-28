# JUDO Runtime Core :: Default Access Manager

Default implementation of access control with behaviour-based authorization.

## Overview

This module provides `DefaultAccessManager`, the standard implementation of the AccessManager interface. It authorizes operations based on actor types, public/private access, and operation behaviours using a set of specialized authorizers.

## Maven Coordinates

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-accessmanager</artifactId>
    <version>${judo-runtime-core-version}</version>
</dependency>
```

**Packaging:** OSGi bundle

## Key Components

### DefaultAccessManager
Main access control implementation that:
- Validates principal tokens
- Checks operation exposure to actors
- Delegates to behaviour-specific authorizers
- Invokes authentication interceptors

```java
DefaultAccessManager accessManager = DefaultAccessManager.builder()
    .asmModel(asmModel)
    .authenticationInterceptorProvider(interceptorProvider)
    .build();

// Authorize an operation call
accessManager.authorizeOperation(operation, signedIdentifier, exchange);
```

### Behaviour Authorizers (`behaviours/` package)

Each authorizer handles a specific operation type:

| Authorizer | Operation Behaviour |
|------------|---------------------|
| `ListAuthorizer` | LIST - Query collections |
| `CreateInstanceAuthorizer` | CREATE_INSTANCE - Create entities |
| `UpdateInstanceAuthorizer` | UPDATE_INSTANCE - Modify entities |
| `DeleteInstanceAuthorizer` | DELETE_INSTANCE - Remove entities |
| `RefreshAuthorizer` | REFRESH - Reload entity data |
| `SetReferenceAuthorizer` | SET_REFERENCE - Set single reference |
| `UnsetReferenceAuthorizer` | UNSET_REFERENCE - Clear reference |
| `AddReferenceAuthorizer` | ADD_REFERENCE - Add to collection |
| `RemoveReferenceAuthorizer` | REMOVE_REFERENCE - Remove from collection |
| `GetReferenceRangeAuthorizer` | GET_REFERENCE_RANGE - Get valid references |
| `GetInputRangeAuthorizer` | GET_INPUT_RANGE - Get input options |
| `GetTemplateAuthorizer` | GET_TEMPLATE - Get default values |

### BehaviourAuthorizer
Base interface for all authorizers:

```java
public interface BehaviourAuthorizer {
    boolean isSuitableForOperation(EOperation operation);
    void authorize(String actorFqName, Collection<String> publicActors, 
                   SignedIdentifier signedIdentifier, EOperation operation);
}
```

## Authorization Flow

1. **Check principal** - Validate authentication token
2. **Check exposure** - Verify operation exposed to actor
3. **Check signed identifier** - Validate bound operation access
4. **Run interceptors** - Execute authentication callbacks
5. **Run authorizers** - Behaviour-specific checks

## Access Control Model

- **Public actors** - Actors without realm requirement
- **Private actors** - Actors requiring authentication
- **Exposed operations** - Operations marked with `@exposedBy`
- **Signed identifiers** - Track operation binding context

## Usage Example

```java
// Build access manager
DefaultAccessManager manager = DefaultAccessManager.builder()
    .asmModel(asmModel)
    .authenticationInterceptorProvider(provider)
    .build();

// In dispatcher or interceptor
Map<String, Object> exchange = new HashMap<>();
exchange.put(Dispatcher.PRINCIPAL_KEY, judoPrincipal);

try {
    manager.authorizeOperation(operation, signedId, exchange);
    // Proceed with operation
} catch (AccessDeniedException e) {
    // Handle access denied
} catch (AuthenticationRequiredException e) {
    // Handle missing authentication
}
```

## Related Modules

- `judo-runtime-core-accessmanager-api` - AccessManager interface
- `judo-runtime-core-security` - Security abstractions
- `judo-dispatcher-api` - Principal and context
