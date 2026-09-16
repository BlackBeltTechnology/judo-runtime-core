# AGENTS.md — `judo-runtime-core-guice/src/main/java/hu/blackbelt/judo/runtime/core/guice/dispatcher`

Guice `Provider<T>` factories composing the dispatcher stack (actor resolution, validation, metrics, variable/function registries).

| File | Purpose |
| --- | --- |
| `DefaultActorResolverProvider.java` | Guice `Provider<ActorResolver>`. `get()` builds `DefaultActorResolver` from injected `DataTypeManager`, `DAO`, `AsmModel`, `AuthenticationInterceptorProvider`. Optional `@ActorResolverCheckMappedActors` flag, default `false`, controls per-call mapped-actor checks. |
| `DefaultDispatcherProvider.java` | Guice `Provider<Dispatcher>` assembling `DefaultDispatcher` across ~20 collaborators; full detail → see `DefaultDispatcherProvider.java.AGENTS.md` |
| `DefaultIdentifierSignerProvider.java` | Guice `Provider<IdentifierSigner>`. `get()` builds `DefaultIdentifierSigner` from `AsmModel`, `IdentifierProvider`, `DataTypeManager`. Optional `@IdentifierSignerSecret` String binding sets the HMAC secret — unbund secret yields unsigned/session-scoped identifiers. |
| `DefaultMetricsCollectorProvider.java` | Guice `Provider<MetricsCollector>`. `get()` builds `DefaultMetricsCollector` from `Context` plus optional `@MetricsCollectorConsumer` `Consumer` (default no-op), `@MetricsCollectorEnabled` (default false), `@MetricsCollectorVerbose` (default false). |
| `DefaultPayloadValidatorProvider.java` | Guice `Provider<PayloadValidator>` building `DefaultPayloadValidator` with `ACCEPT_NON_EMPTY` default; full detail → see `DefaultPayloadValidatorProvider.java.AGENTS.md` |
| `DefaultVariableResolverProvider.java` | Guice `Provider<VariableResolver>` registering `SYSTEM`/`ENVIRONMENT`/`SEQUENCE`/`REQUEST` suppliers; full detail → see `DefaultVariableResolverProvider.java.AGENTS.md` |
| `DispatcherFunctionProviderProvider.java` | Guice `Provider<DispatcherFunctionProvider>`. `get()` returns anonymous impl holding empty `HashMap` registries; `getSdkFunctions()` and `getScriptFunctions()` expose `Map<EOperation, Function<Payload, Payload>>` for later population. No injected deps. |
| `OperationCallInterceptorProviderProvider.java` | Guice `Provider<OperationCallInterceptorProvider>`. `get()` returns anonymous impl with an initially empty `ArrayList`; `getCallOperationInterceptors()` returns `List<OperationCallInterceptor>` that callers may mutate. |
| `ThreadContextProvider.java` | Guice `Provider<Context>`. `get()` returns `new ThreadContext(debugThreadFork, inheritableContext, dataTypeManager)` from `DataTypeManager` and optional `@ThreadContextDebugThreadFork` (default false), `@ThreadContextInheritableContext` (default true). One new `Context` per `get()`. |
| `ValidatorProviderProvider.java` | Guice `Provider<ValidatorProvider>`. `get()` returns `new DefaultValidatorProvider(dao, identifierProvider, asmModel, context)` — no builder, no optional bindings. |