# `JudoTestExtension.java`

Engine behind the `@JudoTest` meta-annotation. Implements `BeforeAllCallback`,
`AfterAllCallback`, `BeforeEachCallback`, `AfterEachCallback` and `ParameterResolver`,
and owns three JVM-wide `ConcurrentHashMap` caches: `singletonDatasources` keyed by
`SingletonDatasourceKey`, `singletonModelLoaders` keyed by `SingletonModelKey`, and
`singletonRuntimes` keyed by `ByClassCacheKey`.

At 749 lines this file is documented at signature level (contract R5).

## Key behaviour

- **Annotation lookup order** — every callback reads `@JudoTest` from the test *method*
  first and falls back to the test *class*; a class-level hit sets `isClassLevel = true`,
  which is what `JudoTestExtensionRouting.useCache(...)` consumes. A method-level
  annotation therefore never uses the cache.
- **`resolveDialectName(JudoTest)`** — env var `JUDO_TEST_DIALECT` > `annotation.dialect()`
  > `"hsqldb"`; blank strings count as unset.
- **`resolveContainerName(JudoTest, String)`** — env var `JUDO_TEST_CONTAINER` >
  `annotation.container()` > `"none"`, then auto-detects: dialect `postgresql` with
  container `none` is promoted to container `postgresql`. It is extracted precisely so the
  `SingletonDatasourceKey` component mirrors what actually configures the datasource.
- **`beforeAll`** — `initializeDatasource(...)` and `initializeModelLoader(...)` run only for
  class-scoped modes, so `BY_METHOD` pays nothing at class level.
- **`beforeEach`** — routes through `getOrBuildCachedRuntime(...)` when routing says cache,
  otherwise the cold path `prepare(...)` → `init(...)` on a fresh `JudoRuntimeFixture`.
- **`afterEach`** — switches on the stored `TransactionHandling`: `AUTO_ROLLBACK` rolls back;
  `AUTO_COMMIT` commits and, when `truncateTables()` is true, truncates via the datasource
  fixture's `RdbmsModel`; `MANUAL` only truncates; `NONE` does nothing. `tearDown()` always
  runs afterwards. Errors in cleanup are logged, never rethrown — a cleanup failure must not
  mask the test result.
- **Datasource disposal** — the stored `CloseableDatasourceFixture` is removed and closed for
  method-level or `BY_METHOD` runs, but never for `SINGLETON`.
- **`supportsParameter`/`resolveParameter`** — injects exactly `JudoRuntimeFixture` and
  `JudoDatasourceFixture` (exact type match, not `isAssignableFrom`).
- **Test seams** — `singletonRuntimesForTesting()`, `singletonModelLoadersForTesting()`,
  `singletonDatasourcesForTesting()` and `coldPathModelLoader(boolean, Store)` are
  package-private views for the caching regression tests.
- **`registerSingletonShutdownHook` / `closeAllSingletonRuntimes`** — SINGLETON runtimes
  outlive every JUnit store, so they are closed from a JVM shutdown hook instead.

## Contracts a caller can violate

- Mutating a fixture obtained on the cached path leaks state into later test methods in the
  same class: `shareInjector = true` means the *same* injector, not a copy.
- Choosing `TransactionHandling.MANUAL` makes the test responsible for commit/rollback;
  table truncation still happens if `truncateTables()` is left at its default `true`.
