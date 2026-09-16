# `JudoRuntimeFixture.java`

Builds the Guice injector over a loaded JUDO model and drives test transactions. This is
the object injected into a `@JudoTest` method parameter.

## Key exports

- `prepare(String modelName, DataSource, String dialectName)` and the
  `(…, JudoTest.ModelSource)` overload — load model, derive `QueryFactory`, pick the
  dialect module, run Liquibase.
- `prepareWithModel(JudoModelLoader preloadedModel, DataSource, String dialectName)` — same
  but skips model loading, used by the caching fast path.
- `static loadModel(String modelName, String dialectName, JudoTest.ModelSource)` — public so
  a model can be loaded once and cached independently of a fixture instance.
- `init(Module module, Object injectModulesTo)` — creates the injector via
  `Guice.createInjector(Modules.override(...))` and member-injects the test instance.
- `getInjector()`, `addInterceptor(Class<? extends OperationCallInterceptor>)`,
  `addInterceptor(OperationCallInterceptor)`, `getInterceptorProvider()`.
- `beginTransaction()`, `commitTransaction()`, `rollbackTransaction()`, `createSavePoint()`,
  `rollbackToSavePoint(Object)`, `tearDown()`.
- Constants `MODEL_SOURCES = "target/generated-test-sources/model"`, `DIALECT_HSQLDB`,
  `DIALECT_POSTGRESQL`; public field `modelHolder` (`JudoModelLoader`).

## Contracts a caller can violate

- `getInjector()` before `init(...)` throws
  `IllegalStateException("Injector has not been initialized. Call init() first.")`.
- `addInterceptor(...)` after `init(...)` throws `IllegalStateException` and directs the
  caller to `getInterceptorProvider().addInterceptor()` instead; a null argument throws
  `IllegalArgumentException`.
- `beginTransaction()` while a transaction is already open throws
  `IllegalStateException("Previous transaction was not completed")`.
- `rollbackToSavePoint(...)` throws `IllegalStateException` when the `TransactionStatus` is
  null or already completed.
- An interceptor class registered by `Class` must have a public no-arg constructor —
  otherwise instantiation fails with a `RuntimeException`.
- An unknown `dialectName` throws `IllegalArgumentException("Unsupported dialect: …")`, and a
  model that resolves to nothing throws `IllegalArgumentException("Could not load model …")`.
