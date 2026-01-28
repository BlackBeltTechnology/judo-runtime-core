# JUDO Runtime Core :: CXF JAX-RS Integration

Apache CXF-specific JAX-RS interceptors and handlers for JUDO applications.

## Overview

This module provides Apache CXF interceptors for authentication, authorization, fault handling, and request correlation. It integrates JUDO's security model with CXF's interceptor chain.

## Maven Coordinates

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-jaxrs-cxf</artifactId>
    <version>${judo-runtime-core-version}</version>
</dependency>
```

**Packaging:** OSGi bundle

## Key Components (`interceptors/` package)

### JudoAuthorizingInterceptor
CXF authorization interceptor that validates operation access based on ASM model.

```java
JudoAuthorizingInterceptor interceptor = JudoAuthorizingInterceptor.builder()
    .asmModel(asmModel)
    .build();
```

Features:
- Extends `AbstractAuthorizingInInterceptor`
- Reads `@JudoOperation` annotation from methods
- Determines required roles from `@exposedBy` annotations
- Supports public actors (no authentication required)

### FaultInterceptor
Handles exceptions in the CXF message flow, converting them to appropriate responses.

```java
public class FaultInterceptor extends AbstractPhaseInterceptor<Message> {
    // Intercepts faults and transforms to proper HTTP responses
}
```

### ExchangeIdDecorator
Decorates requests with unique exchange IDs for tracing and correlation.

```java
public class ExchangeIdDecorator extends AbstractPhaseInterceptor<Message> {
    // Adds exchange ID to message for request tracking
}
```

### ExchangeIdResponseWriter
Writes exchange ID to response headers for client correlation.

```java
public class ExchangeIdResponseWriter extends AbstractPhaseInterceptor<Message> {
    // Adds X-Exchange-Id header to responses
}
```

## Authorization Flow

1. Request arrives at CXF endpoint
2. `JudoAuthorizingInterceptor` extracts operation from `@JudoOperation`
3. Looks up operation in ASM model
4. Determines exposed actors from `@exposedBy` annotations
5. Checks if actor is public (no realm) or matches authenticated principal
6. Returns required roles (empty for public, actor FQNames for private)

## Usage Example

```java
// Configure CXF endpoint with interceptors
JAXRSServerFactoryBean factory = new JAXRSServerFactoryBean();
factory.setAddress("/api");
factory.setServiceBean(myResource);

// Add authorization interceptor
factory.getInInterceptors().add(
    JudoAuthorizingInterceptor.builder()
        .asmModel(asmModel)
        .build()
);

// Add exchange ID tracking
factory.getInInterceptors().add(new ExchangeIdDecorator());
factory.getOutInterceptors().add(new ExchangeIdResponseWriter());

// Add fault handling
factory.getOutFaultInterceptors().add(new FaultInterceptor());

Server server = factory.create();
```

## Integration with Guice

When using `judo-runtime-core-guice-cxf`:

```java
// Interceptors are automatically configured
Injector injector = Guice.createInjector(
    new JudoDefaultModule(...),
    new JudoCxfModule(...)
);
```

## Request Correlation

Exchange IDs enable:
- Request tracing across services
- Log correlation
- Error tracking
- Performance monitoring

Response headers include:
```
X-Exchange-Id: <uuid>
```

## Dependencies

- `cxf-core` - CXF core interceptor framework
- `cxf-rt-transports-http` - HTTP transport
- `jakarta.ws.rs-api` - JAX-RS API
- `judo-meta-asm` - ASM model access

## Related Modules

- `judo-runtime-core-jaxrs` - Generic JAX-RS providers
- `judo-runtime-core-guice-cxf` - Guice + CXF integration
- `judo-runtime-core-guice-jetty` - Jetty server integration
- `judo-runtime-core-security-keycloak-cxf` - Keycloak + CXF security
