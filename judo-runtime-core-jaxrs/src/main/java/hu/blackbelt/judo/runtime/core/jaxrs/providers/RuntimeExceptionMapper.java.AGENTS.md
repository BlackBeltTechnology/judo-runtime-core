# `RuntimeExceptionMapper.java`

JAX-RS `ExceptionMapper<RuntimeException>` classifying failures per type: `ClientErrorException` passthrough of its response,
`WebApplicationException` → `400` with code `INVALID_JSON` for Jackson causes else `500`, `BusinessException` → `422` plus
`X-Fault` header with `getType()` and `code` from `getErrorCode()`, anything else → `500` `INTERNAL_SERVER_ERROR`.
Builder flags `returnRuntimeExceptions` (embeds stack trace in details) and `includeBusinessCause` (embeds cause)
default `false` — callers must opt in.