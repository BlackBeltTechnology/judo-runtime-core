# JUDO Runtime Core - Dispatcher

## Overview

The Dispatcher module is the central routing component in JUDO applications. It handles operation routing, request/response processing, and provides extension points for interceptors and variable resolution.

## Key Concepts

- **Operations**: CRUD behaviors, scripts, SDK implementations
- **Interceptors**: Pre/post hooks for any operation
- **Variable Resolvers**: Provide context variables (user, timestamp, etc.)
- **Behaviours**: Built-in CRUD operations (create, update, delete, list, etc.)

## Module Information

| Property | Value |
|----------|-------|
| **GroupId** | `hu.blackbelt.judo.runtime` |
| **ArtifactId** | `judo-runtime-core-dispatcher` |
| **Package** | `hu.blackbelt.judo.runtime.core.dispatcher` |

## Package Structure

```
hu.blackbelt.judo.runtime.core.dispatcher/
├── DefaultDispatcher.java           # Main dispatcher implementation
├── OperationCallInterceptor.java    # Interceptor interface
├── OperationCallInterceptorProvider.java
├── DispatcherFunctionProvider.java  # Custom functions interface
├── VariableResolverManager.java     # Variable resolution
├── behaviours/                      # CRUD behavior implementations
│   ├── CreateInstanceCall.java
│   ├── UpdateInstanceCall.java
│   ├── DeleteInstanceCall.java
│   ├── ListCall.java
│   └── ...
├── security/                        # Actor resolution, signing
│   ├── ActorResolver.java
│   └── IdentifierSigner.java
├── environment/                     # Variable providers
│   ├── CurrentDateProvider.java
│   ├── CurrentTimeProvider.java
│   ├── PrincipalVariableProvider.java
│   └── ...
└── context/                         # Thread context management
    └── ThreadContext.java
```

## Extension Points

| Interface | Purpose |
|-----------|---------|
| `OperationCallInterceptor` | Hook into any operation pre/post execution |
| `OperationCallInterceptorProvider` | Provide multiple interceptors |
| `ActorResolver` | Resolve current user/actor from context |
| `VariableResolverManager` | Manage context variables for expressions |
| `DispatcherFunctionProvider` | Add custom dispatcher functions |

## Dependencies

- `judo-dao-api` - DAO interfaces
- `judo-dispatcher-api` - Dispatcher interfaces  
- `judo-meta-asm` - ASM metamodel
- `judo-meta-expression` - Expression model

## Related Modules

| Module | Relationship |
|--------|-------------|
| `judo-runtime-core-guice` | Guice DI integration |
| `judo-runtime-core-spring` | Spring integration |
| `judo-runtime-core-guice-testkit` | Testing utilities |
| `judo-runtime-core-dao-rdbms` | Database access layer |
| `judo-runtime-core-validator` | Validation framework |

## Quick Start

### Creating an Interceptor

```java
public class MyInterceptor implements OperationCallInterceptor {
    @Override
    public String getName() { return "my-interceptor"; }
    
    @Override
    public Object postCall(EOperation op, Object input, Object result) {
        // React to operation result
        return result;
    }
}
```

### Registration with Guice

```java
Multibinder<OperationCallInterceptor> interceptors = 
    Multibinder.newSetBinder(binder(), OperationCallInterceptor.class);
interceptors.addBinding().to(MyInterceptor.class);
```

## Skills Available

| Skill | Description |
|-------|-------------|
| `create-interceptor` | Step-by-step guide to create operation interceptors |
| `dispatcher-architecture` | Understanding dispatcher internals |
| `debug-operations` | Troubleshooting operation issues |

## Further Reading

- `architecture.md` - Detailed architecture diagrams
- `extension-points.md` - Complete extension point reference
- `examples/` - Code examples
