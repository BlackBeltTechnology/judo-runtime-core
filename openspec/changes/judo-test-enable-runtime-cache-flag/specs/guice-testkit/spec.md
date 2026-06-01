## ADDED Requirements

### Requirement: @JudoTest Exposes a `cacheRuntime` Flag
The `@JudoTest` annotation SHALL expose a new optional `boolean cacheRuntime` element with default value `true` that controls whether `JudoTestExtension` caches the derived runtime artifacts (`QueryFactory`, database `Module`, Liquibase executor, Guice `Injector`, `PlatformTransactionManager`) for `dataSourceMode = BY_CLASS` and `dataSourceMode = SINGLETON`.

#### Scenario: Default value preserves existing caching behavior
- **GIVEN** a test class annotated `@JudoTest(dataSourceMode = BY_CLASS)` without specifying `cacheRuntime`
- **WHEN** the test methods execute
- **THEN** `cacheRuntime` SHALL default to `true`
- **AND** `JudoRuntimeFixture#getInjector()` SHALL return the SAME `Injector` instance for every method (identical to the behavior introduced by `cache-byclass-test-runtime`)

#### Scenario: Explicit cacheRuntime = true preserves caching
- **GIVEN** a test class annotated `@JudoTest(dataSourceMode = BY_CLASS, cacheRuntime = true)`
- **WHEN** the test methods execute
- **THEN** the cached runtime SHALL be reused across every method (same identity guarantees as the default case)

### Requirement: cacheRuntime = false Bypasses Runtime Cache for BY_CLASS
For a test class annotated `@JudoTest(dataSourceMode = BY_CLASS, cacheRuntime = false)`, `JudoTestExtension` SHALL build a fresh `QueryFactory`, database `Module`, Liquibase executor, Guice `Injector`, and `PlatformTransactionManager` for every test method, while still reusing the cached `JudoModelLoader` and the per-class `DataSource`.

#### Scenario: BY_CLASS with cacheRuntime = false yields distinct injectors
- **GIVEN** a test class annotated `@JudoTest(dataSourceMode = BY_CLASS, cacheRuntime = false)` with two methods
- **WHEN** both methods execute
- **THEN** `JudoRuntimeFixture#getInjector()` SHALL return DIFFERENT `Injector` instances for the two methods

#### Scenario: BY_CLASS with cacheRuntime = false reuses the DataSource
- **GIVEN** a test class annotated `@JudoTest(dataSourceMode = BY_CLASS, cacheRuntime = false)` with two methods
- **WHEN** both methods execute
- **THEN** the underlying `JudoDatasourceFixture` SHALL be the SAME instance for both methods (per-class scope preserved)

#### Scenario: BY_CLASS with cacheRuntime = false reuses the JudoModelLoader
- **GIVEN** a test class annotated `@JudoTest(dataSourceMode = BY_CLASS, cacheRuntime = false)` with two methods
- **WHEN** both methods execute
- **THEN** `JudoRuntimeFixture#modelHolder` SHALL be the SAME `JudoModelLoader` instance for both methods (per-class model load preserved)

#### Scenario: BY_CLASS with cacheRuntime = false invokes JudoRuntimeFixture#init per method
- **GIVEN** a test subclass of `JudoRuntimeFixture` overriding `init(Module, Object)` and a test class annotated `@JudoTest(dataSourceMode = BY_CLASS, cacheRuntime = false)` configured to use that subclass
- **WHEN** the test methods execute
- **THEN** the overridden `init(\u2026)` SHALL be invoked exactly once per method (the cached fast path is bypassed)

### Requirement: cacheRuntime is a No-Op for SINGLETON

The `cacheRuntime` flag SHALL be honoured **only** for `dataSourceMode = BY_CLASS`. For
`dataSourceMode = SINGLETON`, the flag SHALL be ignored and caching SHALL always be
enabled, mirroring the existing rule that already applies to `BY_METHOD` (where there
is nothing to cache).

This restriction is intentional and follows the v2 proposal
(`proposal-v2-simplify-cacheRuntime.md`): `SINGLETON + cacheRuntime = false` would
re-run Liquibase per method against the shared JVM-wide datasource, risking schema
corruption under parallel execution and effectively turning the singleton into a
flaky per-method mode without exposing the user to the underlying hazard. Tests
that want per-method isolation under SINGLETON's shared datasource SHOULD instead:

- reset state via `@BeforeEach` (recommended for stateful interceptors), or
- switch the class to `BY_CLASS` or `BY_METHOD`.

#### Scenario: SINGLETON with cacheRuntime = true caches (default behaviour)
- **GIVEN** a test class annotated `@JudoTest(dataSourceMode = SINGLETON, cacheRuntime = true)`
- **WHEN** the test methods execute
- **THEN** the cached runtime SHALL be reused across every method (same identity guarantees as the default)

#### Scenario: SINGLETON with cacheRuntime = false still caches (flag ignored)
- **GIVEN** a test class annotated `@JudoTest(dataSourceMode = SINGLETON, cacheRuntime = false)` with two methods
- **WHEN** both methods execute
- **THEN** `JudoRuntimeFixture#getInjector()` SHALL return the SAME `Injector` instance for both methods
- **AND** the JVM-wide singleton runtime map SHALL receive a cache entry for the resolved cache key (identical behaviour to `cacheRuntime = true`)

### Requirement: cacheRuntime is a No-Op for BY_METHOD
For `@JudoTest(dataSourceMode = BY_METHOD)`, the value of `cacheRuntime` SHALL have no observable effect: every method SHALL receive a fresh `Injector`, `QueryFactory`, `Module`, Liquibase executor, and `PlatformTransactionManager`, regardless of the flag value.

#### Scenario: BY_METHOD with cacheRuntime = true behaves identically to cacheRuntime = false
- **GIVEN** two test classes A and B with identical `@JudoTest(dataSourceMode = BY_METHOD, \u2026)` configuration except `cacheRuntime = true` in A and `cacheRuntime = false` in B
- **WHEN** both classes execute their methods
- **THEN** A and B SHALL produce identical observable behavior (fresh Injector per method in both)

### Requirement: cacheRuntime is Ignored on Method-Level @JudoTest
When `@JudoTest` is applied at the method level (rather than the class level), the value of `cacheRuntime` SHALL be ignored, mirroring the existing rule that method-level `@JudoTest` always behaves as `BY_METHOD`.

#### Scenario: Method-level cacheRuntime = true does not enable caching
- **GIVEN** a test method annotated `@JudoTest(cacheRuntime = true)` (no class-level `@JudoTest`)
- **WHEN** the method executes
- **THEN** `JudoTestExtension` SHALL behave as if no caching had been requested (a fresh `Injector` is built for the method)
