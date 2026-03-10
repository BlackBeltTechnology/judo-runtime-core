# Extension Points

Complete reference for all dispatcher extension interfaces.

## OperationCallInterceptor

**Purpose**: Hook into any operation for pre/post processing

**Package**: `hu.blackbelt.judo.runtime.core.dispatcher`

**Interface**:
```java
public interface OperationCallInterceptor {
    String getName();
    default Collection<EOperation> getOperations(AsmModel asmModel);
    default boolean async();
    default boolean terminateOnException();
    default boolean ignoreDecoratedCall();
    default Object preCall(EOperation operation, Object parameterPayload);
    default Object postCall(EOperation operation, Object parameterPayload, Object returnPayload);
}
```

**Methods**:

| Method | Required | Default | Description |
|--------|----------|---------|-------------|
| `getName()` | Yes | - | Unique interceptor identifier |
| `getOperations(AsmModel)` | No | `emptyList()` | Filter operations to intercept (empty = all) |
| `async()` | No | `false` | Run on separate thread, outside transaction |
| `terminateOnException()` | No | `true` | Stop execution if this interceptor throws |
| `ignoreDecoratedCall()` | No | `false` | Skip the original DAO call entirely |
| `preCall(operation, payload)` | No | passthrough | Called before operation executes |
| `postCall(operation, input, result)` | No | passthrough | Called after operation executes |

**Example**:
```java
public class AuditInterceptor implements OperationCallInterceptor {
    @Override
    public String getName() { return "audit"; }
    
    @Override
    public Object postCall(EOperation op, Object input, Object result) {
        auditLog.record(op.getName(), input, result);
        return result;
    }
}
```

---

## OperationCallInterceptorProvider

**Purpose**: Provide multiple interceptors from a single source

**Package**: `hu.blackbelt.judo.runtime.core.dispatcher`

**Interface**:
```java
public interface OperationCallInterceptorProvider {
    default Collection<OperationCallInterceptor> getCallOperationInterceptors();
    default Collection<OperationCallInterceptor> getInterceptorsForOperation(
        AsmModel asmModel, EOperation operation);
}
```

**Methods**:

| Method | Description |
|--------|-------------|
| `getCallOperationInterceptors()` | Return all interceptors managed by this provider |
| `getInterceptorsForOperation(asmModel, operation)` | Return interceptors applicable to specific operation |

**Example**:
```java
public class MyInterceptorProvider implements OperationCallInterceptorProvider {
    private final List<OperationCallInterceptor> interceptors;
    
    @Override
    public Collection<OperationCallInterceptor> getCallOperationInterceptors() {
        return interceptors;
    }
}
```

---

## ActorResolver

**Purpose**: Resolve the current user/actor from security context

**Package**: `hu.blackbelt.judo.runtime.core.dispatcher.security`

**Interface**:
```java
public interface ActorResolver {
    Optional<EClass> getActor();
    Optional<Payload> getActorPayload();
    void setActor(EClass actor);
    void setActorPayload(Payload payload);
}
```

**Methods**:

| Method | Description |
|--------|-------------|
| `getActor()` | Get current actor's EClass (type) |
| `getActorPayload()` | Get current actor's data as Payload |
| `setActor(actor)` | Set current actor type |
| `setActorPayload(payload)` | Set current actor data |

**Default Implementation**: `DefaultActorResolver`

**Configuration**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `checkMappedActors` | `Boolean` | `false` | Check/load mapped actors by default |
| `acceptableClients` | `String` | `null` | Additional accepted Keycloak client IDs mapped to actor types. Format: `ActorFQN=client1,client2;OtherActor=client3`. Client values must use dots (not dashes). See [Authentication Flow - Acceptable Clients Whitelist](../../judo-runtime-core-security/src/main/resources/claude/plugins/judo-security/skills/authentication-flow/SKILL.md) for details. |

**Example**:
```java
public class KeycloakActorResolver implements ActorResolver {
    @Override
    public Optional<EClass> getActor() {
        return Optional.ofNullable(SecurityContextHolder.getContext())
            .map(ctx -> resolveActorFromToken(ctx.getAuthentication()));
    }
}
```

---

## VariableResolverManager

**Purpose**: Manage context variables available in expressions

**Package**: `hu.blackbelt.judo.runtime.core.dispatcher`

**Interface**:
```java
public interface VariableResolverManager {
    Optional<Object> resolve(String name);
    void register(String name, Supplier<Object> supplier);
}
```

**Built-in Variables**:

| Variable | Provider | Description |
|----------|----------|-------------|
| `CURRENT_DATE` | `CurrentDateProvider` | Today's date |
| `CURRENT_TIME` | `CurrentTimeProvider` | Current time |
| `CURRENT_TIMESTAMP` | `CurrentTimestampProvider` | Current date+time |
| `PRINCIPAL` | `PrincipalVariableProvider` | Authenticated user |
| `ACTOR` | `ActorVariableProvider` | Current actor context |
| `ENV` | `EnvironmentVariableProvider` | Environment variables |
| `REQUEST_PARAMS` | `RequestParametersVariableProvider` | HTTP request parameters |

---

## EnvironmentVariableProvider

**Purpose**: Provide custom environment variables

**Package**: `hu.blackbelt.judo.runtime.core.dispatcher.environment`

**Interface**:
```java
public interface EnvironmentVariableProvider {
    Optional<Object> get(String name);
}
```

**Example**:
```java
public class CustomEnvProvider implements EnvironmentVariableProvider {
    @Override
    public Optional<Object> get(String name) {
        if ("TENANT_ID".equals(name)) {
            return Optional.of(TenantContext.getCurrentTenant());
        }
        return Optional.empty();
    }
}
```

---

## DispatcherFunctionProvider

**Purpose**: Add custom functions to the dispatcher

**Package**: `hu.blackbelt.judo.runtime.core.dispatcher`

**Interface**:
```java
public interface DispatcherFunctionProvider {
    Map<String, Function<Object[], Object>> getFunctions();
}
```

**Example**:
```java
public class MyFunctionProvider implements DispatcherFunctionProvider {
    @Override
    public Map<String, Function<Object[], Object>> getFunctions() {
        return Map.of(
            "formatCurrency", args -> formatAsCurrency((BigDecimal) args[0]),
            "generateCode", args -> generateUniqueCode((String) args[0])
        );
    }
}
```

---

## IdentifierSigner

**Purpose**: Sign and verify entity identifiers for security

**Package**: `hu.blackbelt.judo.runtime.core.dispatcher.security`

**Interface**:
```java
public interface IdentifierSigner {
    SignedIdentifier sign(Object identifier, EClass type);
    Object verify(SignedIdentifier signed, EClass expectedType);
}
```

**Default Implementation**: `DefaultIdentifierSigner`

---

## Export

**Purpose**: Export data to external formats (e.g., Excel)

**Package**: `hu.blackbelt.judo.runtime.core.dispatcher`

**Interface**:
```java
public interface Export {
    byte[] export(String templateName, Map<String, Object> context);
}
```

**Default Implementation**: `UnsupportedExportImpl` (throws exception)

**JXLS Implementation**: See `judo-runtime-core-export-jxls` module

---

## Registration Summary

### Guice Registration

```java
public class MyDispatcherModule extends AbstractModule {
    @Override
    protected void configure() {
        // Interceptors
        Multibinder<OperationCallInterceptor> interceptors = 
            Multibinder.newSetBinder(binder(), OperationCallInterceptor.class);
        interceptors.addBinding().to(MyInterceptor.class);
        
        // Interceptor providers
        Multibinder<OperationCallInterceptorProvider> providers =
            Multibinder.newSetBinder(binder(), OperationCallInterceptorProvider.class);
        providers.addBinding().to(MyInterceptorProvider.class);
        
        // Variable providers
        Multibinder<EnvironmentVariableProvider> envProviders =
            Multibinder.newSetBinder(binder(), EnvironmentVariableProvider.class);
        envProviders.addBinding().to(MyEnvProvider.class);
        
        // Function providers
        Multibinder<DispatcherFunctionProvider> funcProviders =
            Multibinder.newSetBinder(binder(), DispatcherFunctionProvider.class);
        funcProviders.addBinding().to(MyFunctionProvider.class);
        
        // Custom actor resolver
        bind(ActorResolver.class).to(MyActorResolver.class);
    }
}
```

### Spring Registration

```java
@Configuration
public class DispatcherConfig {
    
    @Bean
    public OperationCallInterceptor myInterceptor() {
        return new MyInterceptor();
    }
    
    @Bean
    public EnvironmentVariableProvider myEnvProvider() {
        return new MyEnvProvider();
    }
    
    @Bean
    public ActorResolver actorResolver() {
        return new MyActorResolver();
    }
}
```
