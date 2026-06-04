## Context

`@JudoTest` (in `judo-runtime-core-guice-testkit`) currently caches a single
artifact for `BY_CLASS` / `SINGLETON` modes: the `JudoModelLoader`. Every test
method still runs:

1. `JudoRuntimeFixture#initQueryFactory()` — extracts JQL expressions from the
   ASM model and constructs a `QueryFactory`.
2. `JudoRuntimeFixture#initModules()` — builds an `RdbmsInit` and calls
   `init.execute(datasource)`, which acquires a Liquibase changelog lock,
   re-reads `DATABASECHANGELOG`, and releases the lock (~5 s on a populated DB).
3. `JudoRuntimeFixture#init()` — `Guice.createInjector(...)` over
   `judoDefaultModule + databaseModule + customModule` (~10–15 s).
4. `injector.getInstance(PlatformTransactionManager.class)`.

Inputs to all four steps are constant within a `BY_CLASS` test class
(annotation values are class-level, datasource is shared by an existing
mechanism). The cost is therefore avoidable.

Empirical baseline (rackinspect model, 20 methods, BY_CLASS, HSQLDB,
HikariCP):
- first test: **~28 700 ms**
- cached test avg: **~26 000 ms**
- model-only cache speed-up: **~1.10×**

## Goals / Non-Goals

**Goals**
- Cache `QueryFactory`, `databaseModule`, `Injector`, and
  `PlatformTransactionManager` once per `BY_CLASS` scope; once per JVM for
  `SINGLETON`.
- Skip the second-and-later Liquibase executions for cached scopes.
- Preserve the BY_METHOD isolation invariant (fresh injector per method).
- Preserve the public API surface (`@JudoTest`, `DataSourceMode` enum).
- Provide a counting Liquibase executor seam for regression tests
  (R4 — exact-once Liquibase execution).
- Achieve ≥ 5× cached-avg / first-test speed-up on rackinspect.

**Non-Goals**
- Caching across JVMs (no model serialization).
- Schema fingerprinting to safely re-run Liquibase only when needed.
- Sharing cached runtimes across distinct test classes with different
  configurations (would essentially upgrade BY_CLASS to SINGLETON — separate
  proposal).
- Changing the `BY_METHOD` code path.

## Decisions

### D1. Cache key

Use a value-equal key including every input that can vary:

```java
record ByClassCacheKey(
    String modelName,
    String dialect,
    JudoTest.ModelSource modelSource,
    Class<?> testClass,
    List<Class<? extends Module>> modules,
    List<Class<? extends OperationCallInterceptor>> interceptors
) {}
```

Rationale: prevents accidental cross-test reuse if two classes happen to
share a name or dialect but differ in custom modules/interceptors.

Alternative considered: key by `Class<?>` only. Rejected because
`@JudoTest` config can vary on inheritance / nested classes.

### D2. Cache scope

- **BY_CLASS**: store under `ExtensionContext.Store(Namespace.create(JudoTestExtension.class, testClass))`,
  same scope as the existing `judoDatasourceFixture` and `judoModelLoader`
  entries. JUnit auto-cleans this when the class finishes.
- **SINGLETON**: a static `ConcurrentHashMap<ByClassCacheKey, CachedRuntime>`
  guarded by the same lock as the existing `singletonModelLoader`. Registered
  in the JUnit root store as a `Store.CloseableResource` so it is shut down
  exactly once at test-run end.

Rationale: mirrors the existing two-tier pattern in `JudoTestExtension`, so
maintenance burden stays low.

### D3. Cached value

```java
final class CachedRuntime implements ExtensionContext.Store.CloseableResource {
    final JudoModelLoader modelLoader;
    final QueryFactory queryFactory;
    final ExtendableCoercer coercer;
    final Module databaseModule;
    final SimpleLiquibaseExecutor liquibaseExecutor;
    final Injector injector;
    final PlatformTransactionManager transactionManager;

    @Override public void close() {
        // Best-effort: dispose injector + datasource pool once.
    }
}
```

### D4. Fast path on `JudoRuntimeFixture`

```java
public void prepareWithCachedRuntime(CachedRuntime cached, Object injectModulesTo) {
    this.modelHolder        = cached.modelLoader;
    this.dialect            = ...derived from modelLoader/dialect...;
    this.queryFactory       = cached.queryFactory;
    this.coercer            = cached.coercer;
    this.databaseModule     = cached.databaseModule;
    this.simpleLiquibaseExecutor = cached.liquibaseExecutor;
    this.injector           = cached.injector;
    this.transactionManager = cached.transactionManager;
    if (injectModulesTo != null) {
        injector.injectMembers(injectModulesTo);
    }
}
```

Rationale: keeps the cache logic in `JudoTestExtension` (the orchestrator)
and gives `JudoRuntimeFixture` a clean "install pre-built artifacts" entry
point without duplicating the cold-path logic.

### D5. Liquibase counting wrapper

Introduce `CountingLiquibaseExecutor extends SimpleLiquibaseExecutor` (or a
delegating wrapper) with a package-private `int executionCount()` accessor.
Used only by the cold path of cached scopes. Tests assert
`executionCount() == 1` after N methods.

Alternative considered: spy via Mockito. Rejected because production code
should not depend on Mockito and our existing test infra already prefers
hand-written test doubles for testkit-internal seams.

### D6. Cleanup

`CachedRuntime#close()` invocations:

- BY_CLASS: triggered by JUnit's class-scoped store cleanup.
- SINGLETON: triggered by the existing root-store `CloseableResource`
  registration pattern; multiple `CachedRuntime` instances live in a `Map`
  whose values are all closed in a single root-store `close()`.

`close()` is idempotent: a `closed` boolean prevents double-close on the
unlikely re-entry from JUnit.

### D7. BY_METHOD path

Untouched. `JudoTestExtension.beforeEach` continues to call
`runtimeFixture.prepare(...)` then `runtimeFixture.init(...)` as today, so
existing user subclasses overriding `init(...)` keep working (regression
guard R12).

## Risks / Trade-offs

- **Stateful interceptor instances reused across methods** → Document in
  `agent-docs/TEST-CONFIGURATION.md`. Provide
  `JudoRuntimeFixture#resetInterceptorReferences()` for users who want a
  half-way refresh without losing the cached injector.
- **Schema-mutating tests break under cache** (e.g. tests that drop a table
  expecting re-migration on the next method) → Documented limitation; users
  must switch to `BY_METHOD`. Caught by user, not by the framework.
- **Cache key over-/under-fitting** → Mitigated by including
  `(testClass, modules, interceptors)` in the key (D1). Regression test
  R10 enforces this.
- **Double-close on shutdown** → Idempotent `close()` (D6) plus regression
  test R11.
- **Subclass overriding `init(...)` no longer called on cached path** →
  Acceptable: users who need their override must use `BY_METHOD`. R12
  guards `BY_METHOD` still calls `init(...)`.
- **Hardware-dependent perf assertion (R8)** → Use a conservative 5×
  threshold and an absolute 6 s ceiling; tagged `slow` so it doesn't run on
  laptops in default builds.

## Migration Plan

Pure additive change. Rollout:

1. Land tests R1–R7 + R10–R12 (RED for cache-related ones).
2. Land cache implementation; R1–R7 + R10–R12 turn GREEN.
3. Land R8–R9 perf guard. Run once on the project's CI hardware to confirm
   ≥ 5× speed-up and ≤ 6 s cached avg.
4. Update `agent-docs/TEST-CONFIGURATION.md` and `agent-docs/api-reference.md`
   with caching invariants and limitations.

Rollback: revert PR. Not API-affecting, no schema changes.

## Open Questions

- Should `JudoRuntimeFixture#resetInterceptorReferences()` be public from
  day 1, or wait for a user request? Default: include as `protected`
  package-private now to keep the seam available.
- Is there appetite for a follow-up that shares cached runtimes across
  identically-configured `BY_CLASS` test classes? Out of scope here, but
  worth a follow-up ticket.
