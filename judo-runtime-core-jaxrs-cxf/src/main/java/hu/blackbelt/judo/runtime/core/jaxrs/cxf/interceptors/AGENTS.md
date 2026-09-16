# AGENTS.md — `judo-runtime-core-jaxrs-cxf/src/main/java/hu/blackbelt/judo/runtime/core/jaxrs/cxf/interceptors`

CXF phase interceptors for request exchange-id propagation, fault-to-HTTP mapping, and ASM-driven role authorization.

| File | Purpose |
| --- | --- |
| `ExchangeIdDecorator.java` | CXF `Phase.RECEIVE` interceptor assigning a request exchange id. Exports `KEY_EXCHANGE_ID` (`"exchangeId"`), `handleMessage(Message)`, `createExchangeId(Message)`. `createExchangeId` reuses the id stored on the `Exchange` and mints a `UUID` only when absent; `handleMessage` writes it to MDC key `RequestExchangeId`. |
| `ExchangeIdResponseWriter.java` | CXF `Phase.POST_LOGICAL` interceptor echoing the request exchange id back as an `X-Exchange-Id` response header. Exports `handleMessage(Message)`, `handleFault(Message)`. Reads `ExchangeIdDecorator.KEY_EXCHANGE_ID` from the exchange; header is written only when both exchange and id are non-null, and `handleFault` delegates to `handleMessage`. |
| `FaultInterceptor.java` | `Phase.PRE_STREAM` CXF fault interceptor mapping thrown exceptions to HTTP status and `FaultMode`; `@Setter logException` gates info-vs-debug logging. → see `FaultInterceptor.java.AGENTS.md` |
| `JudoAuthorizingInterceptor.java` | `AbstractAuthorizingInInterceptor` deriving CXF expected roles from the ASM model via `@JudoOperation` and `exposedBy` access points. → see `JudoAuthorizingInterceptor.java.AGENTS.md` |
