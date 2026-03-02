# JAX-RS Providers Specification

## Purpose
Provides a set of JAX-RS provider components for exception mapping, payload serialization, date parameter handling, and default content-type enforcement in JUDO runtime REST APIs.

## Architecture
- **`ClientExceptionMapper`** -- Maps `ClientException` instances to HTTP responses with the appropriate status code and JSON error details.
- **`RuntimeExceptionMapper`** -- Catches all `RuntimeException` types and maps them to structured JSON error responses, handling `ClientErrorException`, `WebApplicationException`, `BusinessException`, and general runtime exceptions differently.
- **`PayloadMessageBodyWriter`** -- A `MessageBodyWriter` that serializes `Payload`/`PayloadImpl` objects to JSON using a configurable Jackson `ObjectMapper`.
- **`ISO8601DateParamHandler`** -- A `ParamConverterProvider` that converts ISO 8601 date strings in JAX-RS path/query parameters to `java.util.Date` objects.
- **`SetDefaultContentTypePreMatchContainerRequestFilter`** -- A pre-matching `ContainerRequestFilter` that sets a default `Content-Type` header on requests that lack one.

### Package
`hu.blackbelt.judo.runtime.core.jaxrs.providers`

## Requirements

### Requirement: Client exception mapping to HTTP response
`ClientExceptionMapper` SHALL map a `ClientException` to an HTTP response using the exception's `statusCode` (or 400 Bad Request if null) with the exception's `details` as a JSON entity.

#### Scenario: ClientException with explicit status code
- **GIVEN** a `ClientException` with `statusCode` of 409 and a details payload
- **WHEN** `ClientExceptionMapper.toResponse()` is invoked
- **THEN** the response has HTTP status 409, content type `application/json`, and the body contains the exception details

#### Scenario: ClientException with null status code
- **GIVEN** a `ClientException` with `statusCode` of null
- **WHEN** `ClientExceptionMapper.toResponse()` is invoked
- **THEN** the response has HTTP status 400 (Bad Request)

### Requirement: Optional client exception logging
`ClientExceptionMapper` SHALL log the exception at ERROR level only when the `logException` flag is set to `true`.

#### Scenario: Logging disabled for client exceptions
- **GIVEN** `ClientExceptionMapper` with `logException` set to `false`
- **WHEN** a `ClientException` is mapped
- **THEN** no error log entry is produced

### Requirement: Runtime exception mapping with categorized status codes
`RuntimeExceptionMapper` SHALL map `RuntimeException` subtypes to differentiated HTTP status codes: `ClientErrorException` returns its own response directly, `WebApplicationException` with Jackson cause returns 400 with `INVALID_JSON` code, `BusinessException` returns 422 with fault details, and other `RuntimeException` returns 500 with `INTERNAL_SERVER_ERROR` code.

#### Scenario: BusinessException mapping
- **GIVEN** a `BusinessException` with type `"OrderValidationFault"` and detail entries
- **WHEN** `RuntimeExceptionMapper.toResponse()` is invoked
- **THEN** the response has HTTP status 422, an `X-Fault` header set to `"OrderValidationFault"`, and a JSON body with filtered details (excluding keys starting with `_` and null values)

#### Scenario: WebApplicationException with Jackson deserialization cause
- **GIVEN** a `WebApplicationException` whose cause is a `com.fasterxml.jackson` exception
- **WHEN** `RuntimeExceptionMapper.toResponse()` is invoked
- **THEN** the response has HTTP status 400 and the JSON body contains `"code": "INVALID_JSON"`

#### Scenario: Generic RuntimeException
- **GIVEN** a plain `RuntimeException` (not a known subtype)
- **WHEN** `RuntimeExceptionMapper.toResponse()` is invoked
- **THEN** the response has HTTP status 500 and the JSON body contains `"code": "INTERNAL_SERVER_ERROR"`

### Requirement: Optional stack trace inclusion in error responses
`RuntimeExceptionMapper` SHALL include the exception stack trace in the `details.exception` field of the JSON response when `returnRuntimeExceptions` is `true`, except for `BusinessException` instances.

#### Scenario: Stack trace included for runtime exceptions
- **GIVEN** `RuntimeExceptionMapper` with `returnRuntimeExceptions` set to `true`
- **WHEN** a generic `RuntimeException` is mapped
- **THEN** the response JSON `details` object contains an `exception` key with the stack trace string

#### Scenario: Stack trace excluded for business exceptions
- **GIVEN** `RuntimeExceptionMapper` with `returnRuntimeExceptions` set to `true`
- **WHEN** a `BusinessException` is mapped
- **THEN** the response JSON does not contain a `details.exception` field

### Requirement: Optional business exception cause inclusion
`RuntimeExceptionMapper` SHALL include the cause of a `BusinessException` in the response JSON when `includeBusinessCause` is `true` and the cause is not null.

#### Scenario: Business cause included
- **GIVEN** `RuntimeExceptionMapper` with `includeBusinessCause` set to `true` and a `BusinessException` that has a non-null cause
- **WHEN** the exception is mapped
- **THEN** the response JSON contains a `"cause"` field

### Requirement: Payload serialization to JSON via MessageBodyWriter
`PayloadMessageBodyWriter` SHALL serialize `Payload` and `PayloadImpl` instances to JSON using the configured `ObjectMapper` and write the result to the JAX-RS output stream.

#### Scenario: Writing a PayloadImpl to the response
- **GIVEN** a `PayloadMessageBodyWriter` with a configured `ObjectMapper`
- **WHEN** `writeTo()` is called with a `PayloadImpl` instance
- **THEN** the `ObjectMapper` serializes the payload to the output stream as JSON

#### Scenario: Checking writeability for Payload types
- **GIVEN** a `PayloadMessageBodyWriter`
- **WHEN** `isWriteable()` is called with `Payload.class` or `PayloadImpl.class`
- **THEN** the method returns `true`

### Requirement: ISO 8601 date parameter conversion
`ISO8601DateParamHandler` SHALL provide a `ParamConverter<Date>` for `java.util.Date` types that parses date strings in `yyyy-MM-dd` format.

#### Scenario: Parsing a valid date parameter
- **GIVEN** a JAX-RS endpoint with a `Date` query parameter
- **WHEN** the parameter value `"2024-03-15"` is received
- **THEN** the `DateParameterConverter.fromString()` method returns a `Date` representing March 15, 2024

#### Scenario: Parsing an invalid date parameter
- **GIVEN** a JAX-RS endpoint with a `Date` query parameter
- **WHEN** the parameter value `"not-a-date"` is received
- **THEN** the `DateParameterConverter.fromString()` method throws an `IllegalArgumentException`

### Requirement: Default content-type injection for body-bearing requests
`SetDefaultContentTypePreMatchContainerRequestFilter` SHALL set the `Content-Type` header to `application/json` (or a configured default) on POST, PUT, and PATCH requests that have no `Content-Type` header, and SHALL skip GET and DELETE requests.

#### Scenario: POST request without Content-Type header
- **GIVEN** an incoming POST request with no `Content-Type` header
- **WHEN** `SetDefaultContentTypePreMatchContainerRequestFilter.filter()` processes the request
- **THEN** the `Content-Type` header is set to `application/json`

#### Scenario: GET request without Content-Type header
- **GIVEN** an incoming GET request with no `Content-Type` header
- **WHEN** `SetDefaultContentTypePreMatchContainerRequestFilter.filter()` processes the request
- **THEN** the `Content-Type` header is not modified
