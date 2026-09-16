# AGENTS.md — `judo-runtime-core/src/main/java/hu/blackbelt/judo/runtime/core`

Framework core services: custom data-type converter management, timing metrics collection, Payload tree traversal, and identifier providers for DAO-layer payloads.

| File | Purpose |
| --- | --- |
| `DataTypeManager.java` | Registers and aggregates `Converter`s per `EDataType` for custom types, wiring them into `ExtendableCoercer`. Exports `registerCustomType(EDataType, String, Collection<Converter>, Formatter)`, `unregisterCustomType(EDataType)`, `getCustomTypeName(EDataType)`, `getCoercer()`, `setCoercer()`. Registering same `EDataType` twice throws `IllegalStateException`. |
| `MetricsCancelToken.java` | `AutoCloseable` token returned by `MetricsCollector.start(String)`; `close()` calls `metricsCollector.stop(keyOfMeasurement)`. Exports `close()`. Use in try-with-resources so a started measurement always stops. |
| `MetricsCollector.java` | Interface for collection of nested timing measurements. Exports `FRAMEWORK_METRICS`, `start(String)` returning `MetricsCancelToken`, `stop(String)`, `getMetrics()` returning `Map<String, AtomicLong>`. `stop(key)` throws `IllegalStateException` when `key` does not match last started measurement. |
| `PayloadTraverser.java` | Walks a `Payload` tree along `EClass` references filtered by `predicate`, invoking `processor` on every visited node with a `PayloadTraverserContext` (type + path). Exports `traverse(Payload, EClass)`, nested `PayloadTraverserContext.getPathAsString()`, `ReferenceItem`, `PathEntry`. Null payload returns null; only payload-present references passing `predicate` are descended. |
| `SerializableIdentifierProvider.java` | `IdentifierProvider` producing `UUID.randomUUID()` typed `Serializable.class`, named `__identifier`. Exports `get()`, `getType()`, `getName()`. |
| `UUIDIdentifierProvider.java` | `IdentifierProvider` producing `UUID.randomUUID()`, type `UUID.class`, named `__identifier`. Exports `get()`, `getType()`, `getName()`. |