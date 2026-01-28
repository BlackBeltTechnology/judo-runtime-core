# JUDO CXF Server - Agent Documentation

## Overview

This module provides JAX-RS providers for Apache CXF server configuration:

- **ClientExceptionMapper** - Maps `ClientException` to HTTP responses with proper status codes
- **RuntimeExceptionMapper** - Handles runtime exceptions including `BusinessException` with HTTP 422 responses
- **PayloadMessageBodyWriter** - Serializes `Payload` objects to JSON using Jackson
- **SetDefaultContentTypePreMatchContainerRequestFilter** - Sets default Content-Type to `application/json` for POST/PUT/PATCH requests

## Key Components

### Exception Mappers

| Class | Exception Type | HTTP Status |
|-------|---------------|-------------|
| `ClientExceptionMapper` | `ClientException` | 400 (or custom) |
| `RuntimeExceptionMapper` | `BusinessException` | 422 |
| `RuntimeExceptionMapper` | `WebApplicationException` | 500 |
| `RuntimeExceptionMapper` | Other `RuntimeException` | 500 |

### RuntimeExceptionMapper Configuration

```java
RuntimeExceptionMapper.builder()
    .returnRuntimeExceptions(true)    // Include stack trace in response
    .includeBusinessCause(true)       // Include cause for BusinessException
    .build();
```

### PayloadMessageBodyWriter Usage

```java
PayloadMessageBodyWriter.builder()
    .objectMapper(customObjectMapper)  // Optional custom ObjectMapper
    .build();
```

### Content-Type Filter

The `SetDefaultContentTypePreMatchContainerRequestFilter` automatically sets `Content-Type: application/json` for POST/PUT/PATCH requests when no Content-Type is provided.

```java
SetDefaultContentTypePreMatchContainerRequestFilter.builder()
    .defaultRequestContentType("application/json")  // Customize default
    .build();
```

## Files in This Package

| File | Content |
|------|---------|
| `README.md` | This file - overview and component documentation |
