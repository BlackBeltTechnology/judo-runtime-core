## Why

Tests using `@JudoTest(dataSourceMode = BY_CLASS)` currently rebuild the
`QueryFactory`, run a Liquibase changelog re-validation, and recreate the full
Guice `Injector` for every test method, even though all of these inputs are
class-stable. Empirically, on a 20-method class with a real-world model
(rackinspect, ~20 MB ASM), the existing model-only cache yields just ~10 %
speed-up because the model load is only ~3 s of the ~28 s per-test cost. The
remaining ~25 s is repeated Liquibase work and Guice injector construction,
which can be safely cached at the class level.

This change extends the existing `JudoModelLoader` cache to also cache the
derived runtime artifacts (Guice injector, `QueryFactory`,
`PlatformTransactionManager`, database module, Liquibase execution) for
`BY_CLASS` and `SINGLETON` modes. Target: cached per-test cost drops from
~26 s to ~3–5 s (≥ 5× speed-up on a 20-method class), without changing the
public `@JudoTest` API.

## What Changes

- `JudoTestExtension` SHALL cache derived runtime artifacts (not only the
  `JudoModelLoader`) under a class-scoped key for `BY_CLASS` mode and under
  a static map for `SINGLETON` mode.
- `JudoRuntimeFixture` SHALL gain a fast path
  `prepareWithCachedRuntime(CachedRuntime, Object)` that bypasses
  `initQueryFactory`, `initModules`, and `Guice.createInjector` when valid
  cached values are passed in.
- The `BY_METHOD` code path SHALL remain unchanged (every method continues to
  receive a fresh runtime — isolation invariant preserved).
- The cache key SHALL include `(modelName, dialect, modelSource, testClass,
  modules, interceptors)` so two test classes with different `@JudoTest`
  configs do NOT share a runtime.
- A `Store.CloseableResource` wrapper SHALL ensure that a cached `Injector`
  and its underlying `HikariCP` pool are closed exactly once when the test
  class finishes (BY_CLASS) or at JVM shutdown (SINGLETON).
- A counting wrapper around `SimpleLiquibaseExecutor` SHALL be exposed
  (package-private) so regression tests can assert exactly one Liquibase
  execution per cached runtime.
- New regression-guard tests (R1–R12 in the design doc) SHALL be added to
  `judo-runtime-core-guice-testkit`. The slow performance guards
  (R8, R9) SHALL be tagged `slow` to keep default builds fast.

No public API surface (no annotations, no enums) is changed.

## Capabilities

### New Capabilities

(none — this is an additive enhancement of an existing capability)

### Modified Capabilities

- `guice-testkit`: adds requirements for class-scoped runtime caching in
  `BY_CLASS` / `SINGLETON` datasource modes; adds requirements for the
  `BY_METHOD` isolation invariant and for a counting Liquibase executor used
  by regression tests.

## Impact

- **Code touched**:
  - `judo-runtime-core-guice-testkit`:
    `fixture/JudoTestExtension.java`,
    `fixture/JudoRuntimeFixture.java`,
    new package-private types `CachedRuntime`, `ByClassCacheKey`,
    `CountingLiquibaseExecutor`.
- **APIs**: no change to the public `@JudoTest` annotation or `DataSourceMode`
  enum. New non-public helper types only. New package-private accessors on
  `JudoRuntimeFixture` (`queryFactory`, `transactionManager`) for tests.
- **Dependencies**: none added. Optional test-only dependency on
  `hu.blackbelt.rackinspect:rackinspect-application-model` already exists for
  the existing performance test and is reused by the tightened regression
  guards.
- **Build / CI**: `mvn test` runs the fast correctness guards; perf guards
  (`@Tag("slow")`) remain opt-in via `-Dgroups=slow` or a CI nightly profile.
- **Risk**: tests that mutate schema or assume a fresh injector per method
  must use `BY_METHOD`. Documented in `agent-docs/TEST-CONFIGURATION.md`.
