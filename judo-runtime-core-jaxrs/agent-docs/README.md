# JUDO Runtime Core :: JAX-RS Providers

JAX-RS provider implementations for REST API support in JUDO applications.

## Overview

This module provides JAX-RS providers for exception mapping, message body writing, content type handling, and date parameter parsing. These providers integrate JUDO's exception and payload handling with standard JAX-RS infrastructure.

## Maven Coordinates

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-jaxrs</artifactId>
    <version>${judo-runtime-core-version}</version>
</dependency>
```

**Packaging:** OSGi bundle (with `X-JAXRS-Provider: true` header)

## Key Components (`providers/` package)

### ClientExceptionMapper
Maps `ClientException` to appropriate HTTP responses.

```java
@Provider
public class ClientExceptionMapper implements ExceptionMapper<ClientException> {
    @Override
    public Response toResponse(ClientException exception) {
        return Response.status(exception.getStatusCode())
                .entity(exception.getDetails())
                .type("application/json")
                .build();
    }
}
```

Features:
- Configurable exception logging
- Returns exception details as JSON
- Uses exception's status code or defaults to 400 Bad Request

### RuntimeExceptionMapper
Maps uncaught `RuntimeException` to HTTP 500 responses.

```java
@Provider
public class RuntimeExceptionMapper implements ExceptionMapper<RuntimeException> {
    // Converts runtime exceptions to internal server error responses
}
```

### PayloadMessageBodyWriter
Writes JUDO payload maps to HTTP response body.

```java
@Provider
public class PayloadMessageBodyWriter implements MessageBodyWriter<Map<String, Object>> {
    // Serializes payload using Jackson ObjectMapper
}
```

### SetDefaultContentTypePreMatchContainerRequestFilter
Pre-matching filter that sets default `Content-Type` header if missing.

```java
@Provider
@PreMatching
public class SetDefaultContentTypePreMatchContainerRequestFilter 
        implements ContainerRequestFilter {
    // Ensures Content-Type: application/json for requests without header
}
```

### ISO8601DateParamHandler
Handles ISO-8601 date string parameters in JAX-RS endpoints.

## Usage Example

```java
// Register providers with JAX-RS application
@ApplicationPath("/api")
public class JudoApplication extends Application {
    @Override
    public Set<Class<?>> getClasses() {
        Set<Class<?>> classes = new HashSet<>();
        classes.add(ClientExceptionMapper.class);
        classes.add(RuntimeExceptionMapper.class);
        classes.add(PayloadMessageBodyWriter.class);
        classes.add(SetDefaultContentTypePreMatchContainerRequestFilter.class);
        return classes;
    }
}

// Providers automatically handle exceptions
@Path("/orders")
public class OrderResource {
    @POST
    public Response createOrder(Map<String, Object> payload) {
        // If ClientException thrown, ClientExceptionMapper handles response
        dispatcher.callOperation("createOrder", payload);
        return Response.ok().build();
    }
}
```

## HTTP Status Mapping

| Exception Type | HTTP Status |
|---------------|-------------|
| `ClientException` | From exception or 400 |
| `ValidationException` | 400 Bad Request |
| `AccessDeniedException` | 403 Forbidden |
| `AuthenticationRequiredException` | 401 Unauthorized |
| `RuntimeException` | 500 Internal Server Error |

## Dependencies

- `jakarta.ws.rs-api` - JAX-RS API
- `judo-runtime-core` - Core exceptions
- `judo-runtime-core-accessmanager-api` - Security exceptions
- `judo-dispatcher-api` - Dispatcher context

## Related Modules

- `judo-runtime-core-jaxrs-cxf` - CXF-specific integration
- `judo-runtime-core-jackson` - JSON serialization
- `judo-runtime-core-guice-cxf` - Guice + CXF wiring
