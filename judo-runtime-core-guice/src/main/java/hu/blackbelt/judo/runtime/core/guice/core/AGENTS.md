# AGENTS.md — `judo-runtime-core-guice/src/main/java/hu/blackbelt/judo/runtime/core/guice/core`

Guice `Provider` bindings wiring the runtime-core primitives (mapper coercion, `DataTypeManager`, identifier generation) into the injector.

| File | Purpose |
| --- | --- |
| `CoercererProvider.java` | Guice `Provider<Coercer>`. `get()` returns the `@Inject`ed `ExtendableCoercer` field — same shared instance on every call. Requires an `ExtendableCoercer` binding at injector creation; absent binding throws. |
| `DataTypeManagerProvider.java` | Guice `Provider<DataTypeManager>`. `get()` constructs a fresh `new DataTypeManager(coercer)` per call on the `@Inject`ed shared `ExtendableCoercer`. Requires an `ExtendableCoercer` binding; coercions registered after construction do not affect the manager. |
| `ExtendableCoercererProvider.java` | Guice `Provider<ExtendableCoercer>`. `get()` returns a new `DefaultCoercer()` per call — instances are not shared, so coercions registered on one are invisible to clients holding another. |
| `UUIDIdentifierProviderProvider.java` | Guice `Provider<IdentifierProvider>`. `get()` returns a fresh `new UUIDIdentifierProvider()` per call, generating `java.util.UUID`-based identifiers; no shared state across calls. |