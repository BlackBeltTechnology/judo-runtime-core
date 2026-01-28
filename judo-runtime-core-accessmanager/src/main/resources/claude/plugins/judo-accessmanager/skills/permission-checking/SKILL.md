---
name: judo-runtime:permission-checking
description: Detailed guide to the JUDO permission checking flow for CRUD operations. Use when you need to understand how CREATE, UPDATE, DELETE permissions work, debug authorization failures, or implement custom authorizers.
metadata:
  author: BlackBelt Technology
  version: "${project.version}"
---

# JUDO Permission Checking Flow

Complete guide to understanding how permission checking works for CRUD operations in JUDO Runtime Core.

## Overview

JUDO uses a behaviour-based permission system where:
- Each operation type has a dedicated authorizer
- CRUD flags (create, update, delete) control mutation permissions
- SignedIdentifiers track entity provenance for bound operations
- The `permissions` annotation defines allowed operations

## Authorizer Architecture

```mermaid
classDiagram
    class BehaviourAuthorizer {
        <<abstract>>
        +isSuitableForOperation(EOperation) boolean
        +authorize(actorFqName, publicActors, signedIdentifier, operation) void
        #checkCRUDFlag(asmModel, element, flags) void
    }
    
    class CreateInstanceAuthorizer {
        -asmModel: AsmModel
        +isSuitableForOperation() CREATE_INSTANCE, VALIDATE_CREATE
        +authorize() checks CREATE flag
    }
    
    class UpdateInstanceAuthorizer {
        -asmModel: AsmModel
        +isSuitableForOperation() UPDATE_INSTANCE, VALIDATE_UPDATE
        +authorize() checks UPDATE flag on producer
    }
    
    class DeleteInstanceAuthorizer {
        -asmModel: AsmModel
        +isSuitableForOperation() DELETE_INSTANCE
        +authorize() checks DELETE flag on producer
    }
    
    class ListAuthorizer {
        -asmModel: AsmModel
        +isSuitableForOperation() LIST
        +authorize() checks exposedBy on owner
    }
    
    class SetReferenceAuthorizer {
        -asmModel: AsmModel
        +isSuitableForOperation() SET_REFERENCE
        +authorize() checks UPDATE flag on producer
    }
    
    class GetReferenceRangeAuthorizer {
        -asmModel: AsmModel
        +isSuitableForOperation() GET_REFERENCE_RANGE
        +authorize() checks CREATE or UPDATE flag
    }
    
    BehaviourAuthorizer <|-- CreateInstanceAuthorizer
    BehaviourAuthorizer <|-- UpdateInstanceAuthorizer
    BehaviourAuthorizer <|-- DeleteInstanceAuthorizer
    BehaviourAuthorizer <|-- ListAuthorizer
    BehaviourAuthorizer <|-- SetReferenceAuthorizer
    BehaviourAuthorizer <|-- GetReferenceRangeAuthorizer
```

## Permission Checking Flow

```mermaid
sequenceDiagram
    participant AM as AccessManager
    participant Auth as BehaviourAuthorizer
    participant ASM as AsmModel
    
    AM->>Auth: isSuitableForOperation(operation)
    alt Suitable Authorizer
        AM->>Auth: authorize(actorFqName, publicActors, signedId, operation)
        Auth->>ASM: getOwnerOfOperationWithDefaultBehaviour(operation)
        ASM-->>Auth: owner (ENamedElement)
        
        alt Has SignedIdentifier
            Auth->>Auth: Get producedBy from signedIdentifier
            Auth->>Auth: checkCRUDFlag(asmModel, producer, flags)
        else No SignedIdentifier
            Auth->>Auth: checkCRUDFlag(asmModel, owner, flags)
        end
        
        Auth->>ASM: getExtensionAnnotationByName(element, "permissions")
        ASM-->>Auth: permissions annotation
        
        alt Has Required Permission
            Auth-->>AM: Success (no exception)
        else Missing Permission
            Auth-->>AM: AccessDeniedException
        end
    end
```

## CRUD Flag Checking

The `checkCRUDFlag` method validates permissions:

```java
void checkCRUDFlag(AsmModel asmModel, ENamedElement element, CRUDFlag... flags) {
    // Get permissions annotation from element
    Optional<EAnnotation> permissions = AsmUtils.getExtensionAnnotationByName(
        element, "permissions", false);
    
    // Check if any of the required flags are set
    if (permissions.isEmpty() || 
        Arrays.stream(flags).noneMatch(f -> 
            Boolean.parseBoolean(permissions.get().getDetails().get(f.permissionName)))) {
        
        // Permission denied - throw AccessDeniedException
        throw new AccessDeniedException(ValidationResult.builder()
            .code("PERMISSION_DENIED")
            .level(ValidationResult.Level.ERROR)
            .location(element.getName())
            .details(Map.of("MISSING_PRIVILEGES", Arrays.asList(flags)))
            .build());
    }
}
```

### CRUDFlag Enum

```java
public enum CRUDFlag {
    CREATE("create"),
    UPDATE("update"),
    DELETE("delete");
    
    private final String permissionName;
}
```

## Operation-Specific Authorizers

### CreateInstanceAuthorizer

Handles entity creation operations.

```mermaid
flowchart TD
    A[CREATE_INSTANCE / VALIDATE_CREATE] --> B{Get Owner}
    B --> C[Check CREATE flag on owner]
    C --> D{Owner exposedBy?}
    D -->|Yes| E{Has access annotation?}
    D -->|No| F[Permission Denied]
    E -->|Yes| G[Success]
    E -->|No| H{Has SignedIdentifier?}
    H -->|Yes| I[Check UPDATE on producer]
    H -->|No| J[Cannot check permissions]
```

**Key Logic:**
```java
// Check CREATE permission on owner
checkCRUDFlag(asmModel, owner, CRUDFlag.CREATE);

// Check exposedBy annotation
if (!AsmUtils.annotatedAsTrue(owner, "access")) {
    // If not directly accessible, need UPDATE permission on producer
    checkCRUDFlag(asmModel, signedIdentifier.getProducedBy(), CRUDFlag.UPDATE);
}
```

### UpdateInstanceAuthorizer

Handles entity update operations.

```mermaid
flowchart TD
    A[UPDATE_INSTANCE / VALIDATE_UPDATE] --> B{Has SignedIdentifier?}
    B -->|Yes| C[Get producedBy]
    B -->|No| D[Security Exception]
    C --> E[Check UPDATE flag on producer]
    E --> F{Has Permission?}
    F -->|Yes| G[Success]
    F -->|No| H[Permission Denied]
```

**Key Logic:**
```java
final ETypedElement producer = signedIdentifier.getProducedBy();
if (producer == null) {
    throw new SecurityException("Unable to check permissions");
}
checkCRUDFlag(asmModel, producer, CRUDFlag.UPDATE);
```

### DeleteInstanceAuthorizer

Handles entity deletion operations.

```mermaid
flowchart TD
    A[DELETE_INSTANCE] --> B{Has SignedIdentifier?}
    B -->|Yes| C[Get producedBy]
    B -->|No| D[Security Exception]
    C --> E[Check DELETE flag on producer]
    E --> F{Has Permission?}
    F -->|Yes| G[Success]
    F -->|No| H[Permission Denied]
```

**Key Logic:**
```java
checkCRUDFlag(asmModel, producer, CRUDFlag.DELETE);
```

### Reference Authorizers

All reference operations (SET, UNSET, ADD, REMOVE) require UPDATE permission:

```mermaid
flowchart TD
    A[SET/UNSET/ADD/REMOVE_REFERENCE] --> B{Has SignedIdentifier?}
    B -->|Yes| C[Get producedBy]
    B -->|No| D[Security Exception]
    C --> E[Check UPDATE flag on producer]
```

### GetReferenceRangeAuthorizer

Handles range queries for references, requiring CREATE or UPDATE permission:

```mermaid
flowchart TD
    A[GET_REFERENCE_RANGE] --> B{Has SignedIdentifier?}
    B -->|Yes| C[Get producedBy]
    B -->|No| D[Skip check - allow]
    C --> E[Check CREATE or UPDATE on producer]
```

**Key Logic:**
```java
// Either CREATE or UPDATE allows range access
checkCRUDFlag(asmModel, producer, CRUDFlag.CREATE, CRUDFlag.UPDATE);
```

### ListAuthorizer

Handles list operations by checking exposedBy annotation:

```mermaid
flowchart TD
    A[LIST] --> B[Get Owner]
    B --> C{Owner exposedBy matches actor?}
    C -->|Yes| D[Success]
    C -->|No| E[Permission Denied]
```

**Key Logic:**
```java
if (AsmUtils.getExtensionAnnotationListByName(owner, "exposedBy").stream()
        .noneMatch(a -> publicActors.contains(a.getDetails().get("value")) || 
                        Objects.equals(actorFqName, a.getDetails().get("value")))) {
    throw new SecurityException("Permission denied");
}
```

### GetInputRangeAuthorizer

Handles input range queries with optional signedIdentifier:

```mermaid
flowchart TD
    A[GET_INPUT_RANGE] --> B{Has SignedIdentifier?}
    B -->|Yes| C[Get Owner]
    B -->|No| D[Skip check - allow]
    C --> E{Owner exposedBy matches?}
    E -->|Yes| F[Success]
    E -->|No| G[Permission Denied]
```

### RefreshAuthorizer / GetTemplateAuthorizer

These authorizers perform minimal checks:

- **RefreshAuthorizer**: No permission checks (always allows)
- **GetTemplateAuthorizer**: Only validates owner exists

## SignedIdentifier

The `SignedIdentifier` tracks entity provenance:

```java
@Getter
@Builder
public class SignedIdentifier {
    @NonNull
    private final String identifier;      // Entity ID
    private final ETypedElement producedBy; // Operation/reference that produced this entity
    private final String entityType;       // Entity type name
    private final Integer version;         // Optimistic locking version
    private final Boolean immutable;       // Whether entity is immutable
}
```

### Role in Authorization

```mermaid
graph LR
    subgraph "Entity Access"
        A[List Operation] -->|produces| B[SignedIdentifier]
        C[Get Operation] -->|produces| B
    end
    
    subgraph "Mutation Check"
        B -->|producedBy| D[Check permissions on producer]
        D --> E{Has CRUD flag?}
        E -->|Yes| F[Allow mutation]
        E -->|No| G[Deny mutation]
    end
```

## Permission Annotations

### Model-Level Permissions

```
// ASM model permissions annotation
@permissions(create=true, update=true, delete=false)
transfer OrderItem {
    // Can create and update, but not delete
}
```

### Reference-Level Permissions

```
// Reference with update permission
@permissions(update=true)
relation items: OrderItem[];
```

## Error Details

The `AccessDeniedException` includes detailed information:

```java
throw new AccessDeniedException(ValidationResult.builder()
    .code("PERMISSION_DENIED")
    .level(ValidationResult.Level.ERROR)
    .location(element.getName())
    .details(Map.of(
        "MISSING_PRIVILEGES", Arrays.asList(CRUDFlag.CREATE, CRUDFlag.UPDATE),
        "MODEL_ELEMENT", AsmUtils.getOperationFQName(operation)
    ))
    .build());
```

## Permission Matrix

| Operation | Required Flag | Check Target | SignedId Required |
|-----------|---------------|--------------|-------------------|
| CREATE_INSTANCE | CREATE | owner | Optional |
| VALIDATE_CREATE | CREATE | owner | Optional |
| UPDATE_INSTANCE | UPDATE | producer | Yes |
| VALIDATE_UPDATE | UPDATE | producer | Yes |
| DELETE_INSTANCE | DELETE | producer | Yes |
| SET_REFERENCE | UPDATE | producer | Yes |
| UNSET_REFERENCE | UPDATE | producer | Yes |
| ADD_REFERENCE | UPDATE | producer | Yes |
| REMOVE_REFERENCE | UPDATE | producer | Yes |
| GET_REFERENCE_RANGE | CREATE or UPDATE | producer | Optional |
| GET_INPUT_RANGE | exposedBy | owner | Optional |
| LIST | exposedBy | owner | No |
| REFRESH | None | - | No |
| GET_TEMPLATE | owner exists | owner | No |

## Debugging Permission Issues

### Enable Trace Logging

```xml
<logger name="hu.blackbelt.judo.runtime.core.accessmanager.behaviours" level="TRACE"/>
```

### Common Error Scenarios

| Error | Cause | Solution |
|-------|-------|----------|
| `PERMISSION_DENIED` | Missing CRUD flag | Add required permission annotation |
| `Unable to check permissions` | Null producer in SignedIdentifier | Ensure bound operation provides SignedIdentifier |
| `No owner of operation found` | Invalid operation configuration | Check ASM model operation definition |

### Debugging Checklist

1. Verify `permissions` annotation exists on target element
2. Check that required flag (create/update/delete) is set to true
3. For bound operations, ensure SignedIdentifier is provided
4. Verify `exposedBy` annotations for list/range operations
5. Check producer element has required permissions

## See Also

- `judo-runtime-core-accessmanager-api` - API interfaces
- `/judo-runtime:access-control` - Actor and operation exposure
- `judo-runtime-core-dispatcher` - Operation dispatching
- `judo-runtime-core-dao-core` - DAO layer integration
