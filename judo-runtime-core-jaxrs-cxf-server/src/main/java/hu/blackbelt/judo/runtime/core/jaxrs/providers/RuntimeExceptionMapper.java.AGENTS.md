# `RuntimeExceptionMapper.java`

JAX-RS `ExceptionMapper<RuntimeException>` serializing a JSON error map. `toResponse`:
`ClientErrorException` returns its own `Response` unchanged; `WebApplicationException`
→ 400 `INVALID_JSON` when the cause is a Jackson exception, else 500/400 by cause kind;
`BusinessException` → 422 with fault type in the `X-Fault` header, `_`-prefixed detail
keys dropped, `errorCode` under `code`; anything else → 500 `INTERNAL_SERVER_ERROR`.
Lombok `@Builder` flags `returnRuntimeExceptions` (stack trace under
`details.exception`) and `includeBusinessCause` (adds `cause`).