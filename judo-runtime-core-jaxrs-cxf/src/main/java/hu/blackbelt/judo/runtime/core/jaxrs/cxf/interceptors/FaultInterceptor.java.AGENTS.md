# `FaultInterceptor.java`

CXF `Phase.PRE_STREAM` fault interceptor translating a thrown exception into an
HTTP status and a CXF `FaultMode`.

Exports `handleMessage(Message)`, `handleFault(Message)`, and a `@Setter`
`logException` toggle.

Mapping:

- `InternalServerErrorException` with a `com.fasterxml.jackson` cause → 400
  `CHECKED_APPLICATION_FAULT`; with no cause or a `RuntimeException` cause → 500
  `RUNTIME_FAULT`; otherwise → 400 `UNCHECKED_APPLICATION_FAULT`.
- `ClientException` → its `getStatusCode()`, `CHECKED_APPLICATION_FAULT`.
- `RuntimeException` → 500 `RUNTIME_FAULT`.
- any other (checked) exception → 400 `CHECKED_APPLICATION_FAULT`.

`handleMessage` delegates to `handleFault`. Throws `RuntimeException` when the
exchange carries no exception, and returns early when a `FaultMode` is already
set. Requires the in-message to hold `AbstractHTTPDestination.HTTP_RESPONSE` to
set the status.
