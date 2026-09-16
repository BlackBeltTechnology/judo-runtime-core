# AGENTS.md — `judo-runtime-core-dispatcher/src/main/java/hu/blackbelt/judo/runtime/core/dispatcher/environment`

Suppliers and functions that back JQL environment variables (`SYSTEM`, `ENVIRONMENT`,
`actor`, `principal`, `SEQUENCE`, request parameters). Each one is registered into
`DefaultVariableResolver` under a category, which dispatches `category:key` lookups to them.

| File | Purpose |
| --- | --- |
| `AccessTokenVariableProvider.java` | Resolves access-token claims as `Function<String, Object>`. Reads `Dispatcher.PRINCIPAL_KEY` from `Context`; returns `JudoPrincipal.getAttributes().get(key)`, or `principal.getName()` when the principal is a plain `java.security.Principal` and key equals `name`. Requires `@NonNull context`; any other key on a plain principal logs a warning and yields `null`. |
| `ActorVariableProvider.java` | Resolves actor fields as `Function<String, Object>`. Reads the `Map` stored under `Dispatcher.ACTOR_KEY` in `Context` and returns `actor.get(key)`. Built via `@Builder`/`setContext`; absent actor map yields `null` instead of throwing. |
| `CurrentDateProvider.java` | Supplies `LocalDate.now(zoneId)` as `Supplier<LocalDate>` for the `SYSTEM` date variable. `zoneId` defaults to `ZoneId.systemDefault()` and is settable; value is recomputed per `get()`, so caching must be decided by the resolver registration. |
| `CurrentTimeProvider.java` | Supplies `LocalTime.now(zoneId)` as `Supplier<LocalTime>`, truncated to `ChronoUnit.MILLIS`. `zoneId` defaults to `ZoneId.systemDefault()`; callers must not expect sub-millisecond precision. |
| `CurrentTimestampProvider.java` | Supplies `LocalDateTime.now(zoneId)` as `Supplier<LocalDateTime>`. `zoneId` defaults to `ZoneId.systemDefault()`; unlike `CurrentTimeProvider` it is not truncated. |
| `DefaultVariableResolver.java` | Implements `VariableResolver` + `VariableResolverManager`; routes `category:key` lookups to registered suppliers/functions and coerces the result. → see `DefaultVariableResolver.java.AGENTS.md` |
| `EnvironmentVariableProvider.java` | Resolves OS/JVM environment as `Function<String, Object>`. Returns `System.getenv(key)` when present, otherwise falls back to `System.getProperty(key)`; needs no `Context` and never throws on a missing key. |
| `PrincipalVariableProvider.java` | Resolves principal attributes by invoking the actor's `GET_PRINCIPAL` operation. Reads `Dispatcher.PRINCIPAL_KEY`, resolves `JudoPrincipal.getClient()` to an `EClass` via lazily built `AsmUtils`, calls that operation through `Dispatcher` and returns `payload.get(key)`. Requires `@NonNull` `context`, `asmModel`, `dispatcher`; an unresolvable client fails `checkState` with `Unknown actor:`. |
| `RequestParametersVariableProvider.java` | Resolves per-request parameters as generic `Function<String, T>`. Exports constant `REQUEST_PARAMETERS_KEY = "__requestParameters"`; reads that `Map` from `Context` and returns the entry, `null` when the map or key is absent. Values are cast to the caller's `T` unchecked, so only `String`-valued parameters are safe. |
| `SequenceProvider.java` | Resolves `SEQUENCE` variables as generic `Function<String, T>` by delegating to `sequence.getNextValue(sequenceName)`. Requires `@NonNull Sequence<T> sequence`; every `apply` advances the sequence, so it must never be registered as cacheable. |
