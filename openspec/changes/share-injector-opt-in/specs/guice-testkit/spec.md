# guice-testkit — `shareInjector` opt-in delta

This delta refines the test-runtime caching specification introduced by
`cache-byclass-test-runtime` and supersedes the `cacheRuntime` flag from
`judo-test-enable-runtime-cache-flag`. After both prior changes are
archived together with this one, the final main spec for `guice-testkit`
reflects the requirements in this delta as the authoritative shape.

## REMOVED Requirements

### Requirement: @JudoTest Exposes a `cacheRuntime` Flag
**Reason removed:** Renamed to `shareInjector` (see ADDED Requirement
"@JudoTest Exposes a `shareInjector` Flag").

### Requirement: cacheRuntime = false Bypasses Runtime Cache for BY_CLASS
**Reason removed:** Subsumed by the new ADDED Requirement
"`shareInjector` Default Yields Per-Method Runtime For All Class-Scoped
Modes" — the new default is `false`, so "bypass" is the unmarked path.

### Requirement: cacheRuntime is a No-Op for SINGLETON
**Reason removed:** Reversed. `SINGLETON` now honours the flag (see
ADDED Requirement "`shareInjector` Is Honoured Uniformly For Class-Scoped
Modes"). The carve-out is gone.

### Requirement: cacheRuntime is a No-Op for BY_METHOD
**Reason removed:** Re-stated in ADDED Requirement
"`shareInjector` Is Ignored For BY_METHOD".

### Requirement: cacheRuntime is Ignored on Method-Level @JudoTest
**Reason removed:** Re-stated in ADDED Requirement
"`shareInjector` Is Ignored On Method-Level @JudoTest".

## MODIFIED Requirements

### Requirement: Class-Level Runtime Caching for BY_CLASS Mode
The behaviour previously triggered unconditionally by `dataSourceMode = BY_CLASS` (originally specified in `cache-byclass-test-runtime`) SHALL now be gated on `shareInjector = true`. When `shareInjector = false` (the default), the class-level runtime cache for `BY_CLASS` SHALL NOT be consulted — every test method SHALL receive a freshly constructed `Injector`, `QueryFactory`, `RdbmsInit`, `SimpleLiquibaseExecutor`, and `PlatformTransactionManager`. The shared class-scoped `DataSource` SHALL still be reused.

#### Scenario: BY_CLASS with shareInjector = true caches the injector per class
- **GIVEN** a test class annotated `@JudoTest(dataSourceMode = BY_CLASS,
  shareInjector = true)`
- **WHEN** the test methods execute
- **THEN** `JudoRuntimeFixture#getInjector()` SHALL return the SAME
  `Injector` instance for every method in the class

#### Scenario: BY_CLASS with default (shareInjector = false) builds a fresh injector per method
- **GIVEN** a test class annotated `@JudoTest(dataSourceMode = BY_CLASS)`
  with no explicit `shareInjector`
- **WHEN** the test methods execute
- **THEN** `JudoRuntimeFixture#getInjector()` SHALL return DISTINCT
  `Injector` instances for each method
- **AND** the class-scoped `DataSource` SHALL be reused across methods

### Requirement: JVM-Level Runtime Caching for SINGLETON Mode
The behaviour previously triggered unconditionally by `dataSourceMode = SINGLETON` (originally specified in `cache-byclass-test-runtime`) SHALL now be gated on `shareInjector = true`. When `shareInjector = false` (the default), the JVM-wide runtime cache for `SINGLETON` SHALL NOT be consulted — every test method SHALL receive a freshly constructed `Injector` and ancillary artifacts. The JVM-wide `DataSource` and `JudoModelLoader` keyed by their respective `SingletonDatasourceKey` and `SingletonModelKey` SHALL still be reused.

#### Scenario: SINGLETON with shareInjector = true caches the injector JVM-wide
- **GIVEN** two test classes A and B both annotated
  `@JudoTest(dataSourceMode = SINGLETON, shareInjector = true)` with
  identical model name, dialect, modules, and interceptors
- **WHEN** classes A and B run in the same JVM
- **THEN** `JudoRuntimeFixture#getInjector()` SHALL return the SAME
  `Injector` instance in both classes

#### Scenario: SINGLETON with default (shareInjector = false) builds a fresh injector per method
- **GIVEN** a test class annotated
  `@JudoTest(dataSourceMode = SINGLETON)` with no explicit
  `shareInjector`
- **WHEN** the test methods execute
- **THEN** `JudoRuntimeFixture#getInjector()` SHALL return DISTINCT
  `Injector` instances for each method
- **AND** the JVM-wide `DataSource` (keyed by `SingletonDatasourceKey`)
  SHALL be reused across methods
- **AND** the JVM-wide `JudoModelLoader` (keyed by `SingletonModelKey`)
  SHALL be reused across methods

### Requirement: Liquibase Executes Exactly Once Per Cached Runtime
The "exactly once" guarantee (originally specified in `cache-byclass-test-runtime`) SHALL apply only when `shareInjector = true` (i.e. when a `CachedRuntime` is in play). When `shareInjector = false` against a shared `DataSource` (`BY_CLASS` or `SINGLETON`), `SimpleLiquibaseExecutor#createDatabase(...)` MAY be invoked multiple times. Liquibase MUST nevertheless converge correctly because:

- The `DATABASECHANGELOG` table records applied changesets and the
  executor SHALL emit a no-op for changesets already applied.
- The `DATABASECHANGELOGLOCK` table serialises concurrent invocations
  against the same physical schema.

#### Scenario: shareInjector = true class invokes Liquibase exactly once
- **GIVEN** a 20-method class annotated `@JudoTest(dataSourceMode =
  BY_CLASS, shareInjector = true)` using `CountingLiquibaseExecutor`
- **WHEN** all 20 methods complete
- **THEN** `CountingLiquibaseExecutor#executionCount()` SHALL equal `1`

#### Scenario: shareInjector = false class invokes Liquibase per method, all no-op after the first
- **GIVEN** a 20-method class annotated `@JudoTest(dataSourceMode =
  BY_CLASS)` with no explicit `shareInjector`
- **WHEN** all 20 methods complete
- **THEN** `CountingLiquibaseExecutor#executionCount()` SHALL equal `20`
- **AND** the first invocation SHALL apply the changelog; subsequent
  invocations SHALL be no-ops against `DATABASECHANGELOG`

## ADDED Requirements

### Requirement: @JudoTest Exposes a `shareInjector` Flag
The `@JudoTest` annotation SHALL expose a boolean element named
`shareInjector` that defaults to `false`. The element SHALL be applicable
only at class level; method-level `@JudoTest` annotations SHALL ignore
it.

The element controls **behavioural sharing** between sibling test
methods of a class — specifically, whether they receive the same Guice
`Injector` (and consequently the same `QueryFactory`,
`PlatformTransactionManager`, `SimpleLiquibaseExecutor`, and interceptor
instances bound by that injector). It is orthogonal to
`dataSourceMode`, which controls **resource lifecycle**.

#### Scenario: Default value yields behavioural isolation between methods
- **GIVEN** a class annotated `@JudoTest(dataSourceMode = BY_CLASS)`
  with no explicit `shareInjector`
- **WHEN** any two methods M1 and M2 execute
- **THEN** the `Injector` observed by M1 SHALL NOT be `==` to the
  `Injector` observed by M2

#### Scenario: Explicit shareInjector = true opts into class-level sharing
- **GIVEN** a class annotated `@JudoTest(dataSourceMode = BY_CLASS,
  shareInjector = true)`
- **WHEN** any two methods M1 and M2 execute
- **THEN** the `Injector` observed by M1 SHALL be `==` to the
  `Injector` observed by M2

### Requirement: `shareInjector` Is Honoured Uniformly For Class-Scoped Modes
The framework SHALL apply identical routing logic for `BY_CLASS` and
`SINGLETON` modes: `shareInjector = true` activates the corresponding
runtime cache (class-scoped store or JVM-wide map), and `shareInjector
= false` skips it. The `SINGLETON` mode SHALL NOT have any
flag-ignoring carve-out.

#### Scenario: SINGLETON with shareInjector = false does not consult the runtime cache
- **GIVEN** a class annotated `@JudoTest(dataSourceMode = SINGLETON)`
  with default `shareInjector`
- **WHEN** the first method runs
- **THEN** the `singletonRuntimes` map SHALL NOT receive a new entry
  for this class's cache key
- **AND** `JudoRuntimeFixture#getInjector()` SHALL return a freshly
  built `Injector`

#### Scenario: SINGLETON with shareInjector = true does consult the runtime cache
- **GIVEN** a class annotated `@JudoTest(dataSourceMode = SINGLETON,
  shareInjector = true)`
- **WHEN** the first method runs
- **THEN** the `singletonRuntimes` map SHALL contain a `CachedRuntime`
  entry for this class's cache key after `beforeEach` returns
- **AND** subsequent methods of this class — and any other SINGLETON
  class whose key matches — SHALL reuse the same `Injector`

### Requirement: `shareInjector` Is Ignored For BY_METHOD
The `BY_METHOD` mode SHALL ignore the `shareInjector` flag in both
states. No runtime is ever cached under `BY_METHOD`.

#### Scenario: BY_METHOD with shareInjector = true still rebuilds per method
- **GIVEN** a class annotated `@JudoTest(dataSourceMode = BY_METHOD,
  shareInjector = true)`
- **WHEN** two methods M1 and M2 execute
- **THEN** the `Injector` instances observed by M1 and M2 SHALL be
  distinct

### Requirement: shareInjector Is Ignored On Method-Level Annotations
A `@JudoTest` annotation applied to a method (rather than its enclosing class) SHALL NOT enable injector sharing, regardless of the `shareInjector` value on the method-level annotation. Caching is a class-scoped concept and MUST NOT be activated by a method-level opt-in.

#### Scenario: Method-level shareInjector true does not enable caching
- **GIVEN** a class with no class-level `@JudoTest`, but a method M annotated `@JudoTest(dataSourceMode = BY_CLASS, shareInjector = true)`
- **WHEN** M executes
- **THEN** the routing predicate `useCache(...)` SHALL evaluate to `false`
- **AND** the cold path SHALL build a fresh `Injector` for M

### Requirement: Documentation Frames Sharing As The Opt-In
The user-facing documentation (`README.md`, `TEST-CONFIGURATION.md`, `agent-docs/TEST-CONFIGURATION.md`, `agent-docs/api-reference.md`, `agent-docs/interceptor-testing.md`, `agent-docs/troubleshooting.md`) SHALL describe `shareInjector = true` as the opt-in for behavioural sharing and SHALL place the "caching invariants" warnings (stateful interceptors, schema-mutating tests, shared `@BeforeAll` field state) under the `shareInjector = true` section, not under the `BY_CLASS` mode description. The element name `cacheRuntime` MUST NOT appear in any of these files after the change lands.

#### Scenario: TEST-CONFIGURATION.md describes the default as fresh per method
- **GIVEN** the published `TEST-CONFIGURATION.md`
- **WHEN** a reader follows the documented default `@JudoTest(...)`
  example
- **THEN** the text SHALL state that two test methods receive distinct
  `Injector` instances by default

#### Scenario: api-reference.md documents `shareInjector` (not `cacheRuntime`)
- **GIVEN** the published `api-reference.md`
- **WHEN** a reader searches for the annotation element controlling
  injector sharing
- **THEN** the element name SHALL appear as `shareInjector` and
  `cacheRuntime` SHALL NOT appear anywhere in the file
