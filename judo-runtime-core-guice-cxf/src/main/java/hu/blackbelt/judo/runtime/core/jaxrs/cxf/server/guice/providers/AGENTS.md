# AGENTS.md — `judo-runtime-core-guice-cxf/src/main/java/hu/blackbelt/judo/runtime/core/jaxrs/cxf/server/guice/providers`

Guice `Provider` bindings that construct the CXF server's singleton objects — JAX-RS providers,
filters, interceptors, the object mapper and the CXF servers themselves — from
`CxfConfigurations`-annotated injection points and `CxfQualifiers`-tagged collections.

| File | Purpose |
| --- | --- |
| `ClientExceptionMapperProvider.java` | Guice `Provider<ClientExceptionMapper>`. Optionally injects `@CxfConfigurations.CxfLogException` boolean; `get()` builds a mapper and calls `setLogException(cxfLogException)`. |
| `CrossOriginResourceSharingFilterProvider.java` | Guice `Provider` for the CXF CORS filter; injects eight `@CxfConfigurations.CxfCors*` knobs with defaults but `get()` returns a fresh filter, discarding the configured instance. → see `CrossOriginResourceSharingFilterProvider.java.AGENTS.md` |
| `CxfJaxrsServerProvider.java` | Guice `Provider<ServerHolder>` bootstrapping CXF JAX-RS servers on the Jetty `ServletContextHandler`; wires `@CxfQualifiers` interceptors, providers and features per `Application`, returns `null` for applications with no resources. → see `CxfJaxrsServerProvider.java.AGENTS.md` |
| `ExchangeIdDecoratorProvider.java` | Guice binding handing out the `ExchangeIdDecorator` interceptor from `...cxf.interceptors`; `get()` returns `new ExchangeIdDecorator()`. |
| `ExchangeIdResponseWriterProviderFault.java` | Guice binding for the fault path producing the `ExchangeIdResponseWriter` interceptor from `...cxf.interceptors`; `get()` returns `new ExchangeIdResponseWriter()`. Sibling of `ExchangeIdResponseWriterProviderOut` — distinct binding, same writer type. |
| `ExchangeIdResponseWriterProviderOut.java` | Guice binding for the out path producing the `ExchangeIdResponseWriter` interceptor from `...cxf.interceptors`; `get()` returns `new ExchangeIdResponseWriter()`. Sibling of `ExchangeIdResponseWriterProviderFault` — distinct binding, same writer type. |
| `ExtendedObjectMapperProvider.java` | Guice `Provider<ObjectMapper>`. `getExtendedObjectMapper()` returns a mapper with `findAndRegisterModules()` plus `ParameterNamesModule`, `Jdk8Module`, `JavaTimeModule`, `JSR353Module` registered and `JsonInclude.Include.NON_NULL` serialization; `get()` delegates to it. |
| `FaultInterceptorProvider.java` | Guice binding handing out the `FaultInterceptor` from `...cxf.interceptors`; `get()` returns `new FaultInterceptor()`. |
| `ISO8601DateParamHandlerProvider.java` | Guice `Provider<ISO8601DateParamHandler>`; `get()` returns `new ISO8601DateParamHandler()`. Handler type imports from `...jaxrs.providers`. |
| `JacksonJaxbJsonProviderProvider.java` | Guice `Provider<JacksonJaxbJsonProvider>` wrapping the injected `ObjectMapper` with `DEFAULT_ANNOTATIONS`. `get()` disables `INDENT_OUTPUT`, `WRITE_DATES_AS_TIMESTAMPS` and `FAIL_ON_UNKNOWN_PROPERTIES`. |
| `JudoAuthorizingInterceptorProvider.java` | Guice `Provider<JudoAuthorizingInterceptor>`; injects `JudoModelLoader`, builds via `JudoAuthorizingInterceptor.builder().asmModel(judoModelLoader.getAsmModel())`. |
| `PayloadMessageBodyWriterProvider.java` | Guice `Provider<PayloadMessageBodyWriter>`; `get()` builds via `PayloadMessageBodyWriter.builder().objectMapper(objectMapper)`. Injects `ObjectMapper` plus an optional `@CxfConfigurations.CxfLogException` flag that `get()` never reads. |
| `RuntimeExceptionMapperProvider.java` | Guice `Provider<RuntimeExceptionMapper>`; injects optional `@CxfConfigurations.CxfReturnRuntimeExceptions` and `@CxfConfigurations.CxfIncludeBusinessCause` booleans, `get()` builds with `RuntimeExceptionMapper.builder().returnRuntimeExceptions(...).includeBusinessCause(...)`. |
| `SetDefaultContentTypePreMatchContainerRequestFilterProvider.java` | Guice `Provider<SetDefaultContentTypePreMatchContainerRequestFilter>`; injects optional `@CxfConfigurations.CxfDefaultRequestContentType` string, `get()` builds with `SetDefaultContentTypePreMatchContainerRequestFilter.builder().defaultRequestContentType(...)`. |