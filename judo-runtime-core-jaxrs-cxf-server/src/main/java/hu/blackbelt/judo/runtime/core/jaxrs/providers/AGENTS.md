# AGENTS.md — `judo-runtime-core-jaxrs-cxf-server/src/main/java/hu/blackbelt/judo/runtime/core/jaxrs/providers`

JAX-RS `@Provider` exception mappers and a request filter shaping error responses and request content type for the CXF server.

| File | Purpose |
| --- | --- |
| `ClientExceptionMapper.java` | JAX-RS `ExceptionMapper<ClientException>`. `toResponse` maps `exception.getStatusCode()` (falls back to 400 `BAD_REQUEST`) with `getDetails()` as body, `application/json`. `@Setter logException` (default false) toggles `log.error`, so client errors stay silent unless enabled. |
| `PayloadMessageBodyWriter.java` | JAX-RS `MessageBodyWriter<PayloadImpl>` serializing payloads via `ObjectMapper`. `isWriteable` returns true only when the type class is exactly `Payload.class` or `PayloadImpl.class`. `writeTo` swallows every `Throwable` — failed serialization yields an empty 200 body. |
| `RuntimeExceptionMapper.java` | JAX-RS `ExceptionMapper<RuntimeException>` serializing JSON error maps via `@Builder` flags; full detail → see `RuntimeExceptionMapper.java.AGENTS.md` |
| `SetDefaultContentTypePreMatchContainerRequestFilter.java` | `@PreMatching` `ContainerRequestFilter` setting the `Content-Type` header from `defaultRequestContentType` (default `application/json`) when absent or blank; skips `GET` and `DELETE`. Exports constants `CONTENT_TYPE` and `APPLICTION_JSON`; pre-match ordering relative to other pre-matching filters is not guaranteed. |