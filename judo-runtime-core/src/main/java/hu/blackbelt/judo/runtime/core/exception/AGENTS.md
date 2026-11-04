# AGENTS.md — `judo-runtime-core/src/main/java/hu/blackbelt/judo/runtime/core/exception`

Client-visible runtime exceptions carrying HTTP status codes and `ValidationResult` details for API error mapping.

| File | Purpose |
| --- | --- |
| `AccessDeniedException.java` | `ClientException` subclass for forbidden access. Carries `ValidationResult`; `getStatusCode()` returns 403, `getDetails()` returns the `ValidationResult`. Exports `AccessDeniedException(ValidationResult)`. |
| `AuthenticationRequiredException.java` | `ClientException` subclass for missing/expired credentials. Carries `ValidationResult`; `getStatusCode()` returns 401, `getDetails()` returns the `ValidationResult`. Exports `AuthenticationRequiredException(ValidationResult)`. |
| `ClientException.java` | Abstract `RuntimeException` base for client-error exceptions. Exports `getStatusCode()` and `getDetails()`, both abstract; constructors mirror `RuntimeException` overloads. Subclasses must supply both. |
| `NotFoundException.java` | `ClientException` subclass for missing resources. Carries `ValidationResult`; `getStatusCode()` returns 404, `getDetails()` returns the `ValidationResult`. Exports `NotFoundException(ValidationResult)`. |
| `ValidationException.java` | `ClientException` subclass for validation failures, hanging onto a `Collection<ValidationResult>`. `getStatusCode()` returns 400; `getDetails()` returns the collection; `toString()` appends details. Exports `getValidationResults()`. |