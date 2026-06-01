# Proposal: Cache full runtime artifacts for BY_CLASS / SINGLETON test mode

| Field      | Value                                                            |
|------------|------------------------------------------------------------------|
| Status     | DRAFT — for review                                               |
| Author     | (assign)                                                         |
| JIRA       | JNG-XXXX                                                         |
| Affects    | `judo-runtime-core-guice-testkit`                                |
| Touches    | `JudoTestExtension`, `JudoRuntimeFixture`                        |
| Approach   | TDD (tests first, then implementation, then validation)          |

---

## 1. Background

`@JudoTest` already supports three datasource modes:

- `BY_METHOD` — fresh datasource and runtime per test method
- `BY_CLASS`  — datasource shared across the class
- `SINGLETON` — datasource shared across the JVM

A previous change ([JNG-6374]) cached the heavy `JudoModelLoader` instance for
`BY_CLASS` / `SINGLETON`. However, **only the model is cached**; every test method
still rebuilds:

1. `QueryFactory` (extracts JQL expressions from the ASM model)
2. `RdbmsInit` + a Liquibase changelog re-validation (`init.execute(datasource)`)
3. The `databaseModule` (`JudoHsqldbModule` / `JudoPostgresqlModule`)
4. The full Guice `Injector` (`Guice.createInjector(modules)`)
5. The resolved `PlatformTransactionManager`

Empirical measurement on a 20-method `BY_CLASS` test class with the `rackinspect`
model (`RackinspectModelClassCachePerformanceTest`):

```
test count                       : 20
first test (cold load + setup)   : ~28 700 ms
cached test avg                  : ~26 000 ms
speed-up vs no cache             : ~1.10x
```

The model cache saves only ~10 % because the model load is only **~3 s** of the
~28 s per-test cost. The remaining ~25 s is Liquibase + Guice injector rebuild —
both of which **could be cached** when the inputs are class-stable.

## 2. Goal

Reduce cached-avg per-test cost in `BY_CLASS` mode from **~26 s to ~3–5 s**
(estimated 5–8× speedup), without:

- breaking BY_METHOD isolation guarantees,
- altering the public `@JudoTest` API,
- introducing flaky test interactions caused by shared state.

## 3. What is safe to cache for BY_CLASS

Inputs that are constant for all methods of a `BY_CLASS`-annotated test class:

| Input                          | Constant within class? |
|--------------------------------|------------------------|
| `modelLoader` (cached today)   | ✅                     |
| `dialect`                      | ✅                     |
| `datasource`                   | ✅                     |
| `JudoTest#modules()`           | ✅ (annotation values) |
| `JudoTest#interceptors()`      | ✅ (annotation values) |
| Test instance (for injection)  | ❌ — recreated per test by JUnit |

Therefore the following derived artifacts are safe to cache once per class:

| Artifact                       | Cacheable | Notes |
|--------------------------------|-----------|-------|
| `QueryFactory`                 | ✅        | pure function of the model |
| First Liquibase migration      | ✅        | run once, subsequent are no-ops |
| `databaseModule`               | ✅        | pure function of dialect + datasource |
| Guice `Injector`               | ✅        | depends on the above + class-level `@JudoTest` annotation |
| `PlatformTransactionManager`   | ✅        | resolved from injector once |
| Interceptor instances          | ⚠️ depends on stateful behaviour — see §6 |

Per-test only (must NOT be cached):

- `TransactionStatus` (begin/rollback)
- Test instance reference (used for `injectMembers`)

## 4. Design

### 4.1 Cache key

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

Storage: JUnit `ExtensionContext.Store` keyed under
`Namespace.create(JudoTestExtension.class, testClass)` — same scope where the
model loader is cached today. (For `SINGLETON` mode: a separate static `Map<ByClassCacheKey, CachedRuntime>` guarded as the existing `singletonModelLoader` is.)

### 4.2 Cached value

```java
final class CachedRuntime {
    final JudoModelLoader modelLoader;
    final QueryFactory queryFactory;
    final ExtendableCoercer coercer;
    final Module databaseModule;
    final SimpleLiquibaseExecutor liquibaseExecutor;
    final Injector injector;
    final PlatformTransactionManager transactionManager;
}
```

### 4.3 New fast path in `JudoRuntimeFixture`

```java
/** Reuses already-built runtime artifacts. Caller MUST pass values produced
 *  by an earlier {@link #prepareAndInit(...)} call with identical inputs. */
public void prepareWithCachedRuntime(CachedRuntime cached, Object injectModulesTo) {
    this.modelHolder        = cached.modelLoader;
    this.dialect            = ...;                // derived from modelLoader
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

### 4.4 Updated `JudoTestExtension.beforeEach`

```pseudo
if (mode == BY_CLASS or SINGLETON) {
    cached = store.computeIfAbsent(KEY, k -> buildAndExecuteOnce(annotation, ds));
    runtimeFixture.prepareWithCachedRuntime(cached, testInstance);
} else { /* BY_METHOD: existing path unchanged */ }
runtimeFixture.beginTransaction(); // still per-test
```

`buildAndExecuteOnce` performs the existing `prepareWithModel` + `init` chain
**once** and bundles the resulting artifacts into a `CachedRuntime`.

### 4.5 Liquibase

`init.execute(datasource)` against an already-migrated HSQLDB takes ~5 s for
lock+changelog read. Skipping subsequent runs entirely is safe **because
nothing in the test stack mutates the schema between methods**. If one ever
does (extremely unusual), they should switch to BY_METHOD.

The cache eliminates this cost by simply not invoking `init.execute` again.

## 5. TDD plan

> Per AGENTS.md (rule 7) we drive the change with tests **before** the
> implementation. Each step adds a failing test, then makes it pass.

### Step 0 — baseline (already done)

`RackinspectModelClassCachePerformanceTest` records timings and fails the
assumption when rackinspect is missing. Keep as-is; it'll be the regression
guard for the speedup.

### Step 1 — identity tests for the new cached artifacts (RED)

New test `ByClassRuntimeCacheTest` (does **not** require rackinspect — uses any
trivial model that already loads in CI, or falls back to assumeTrue):

```java
@JudoTest(modelName="rackinspect",
          modelSource = CLASSPATH,
          dataSourceMode = BY_CLASS)
@TestMethodOrder(OrderAnnotation.class)
class ByClassRuntimeCacheTest {

    static Injector firstInjector;
    static QueryFactory firstQueryFactory;
    static PlatformTransactionManager firstTm;

    @Test @Order(1)
    void capture(JudoRuntimeFixture f) {
        firstInjector     = f.getInjector();
        firstQueryFactory = f.queryFactory;
        firstTm           = f.transactionManager;            // package-private accessor or reflection
        assertNotNull(firstInjector);
    }

    @Test @Order(2)
    void injectorIsCached(JudoRuntimeFixture f) {
        assertSame(firstInjector,     f.getInjector(),     "Injector must be reused for BY_CLASS");
        assertSame(firstQueryFactory, f.queryFactory,      "QueryFactory must be reused for BY_CLASS");
        assertSame(firstTm,           f.transactionManager,"TransactionManager must be reused for BY_CLASS");
    }
}
```

Initial run: tests **FAIL** because `getInjector()` currently returns a fresh
instance per method.

### Step 2 — implement the cache (GREEN)

Implement §4 in `JudoTestExtension` and `JudoRuntimeFixture`. Re-run Step 1 →
expect PASS.

### Step 3 — Liquibase-not-re-run (GREEN)

Add an assertion that proves Liquibase is invoked **exactly once** per class.
Hook a `LiquibaseExecutor` test double or spy and verify `execute()` call
count == 1.

```java
class ByClassLiquibaseSingleExecutionTest {
    static int liquibaseExecCount;     // updated by test double
    @Test @Order(1) void m1(...) { /* triggers warm-up */ }
    @Test @Order(2) void m2(...) { /* should NOT re-run liquibase */ }
    @AfterAll
    static void verify() { assertEquals(1, liquibaseExecCount); }
}
```

The instrumentation point: replace `SimpleLiquibaseExecutor` in cached path
with a counting wrapper, or expose a counter on the executor itself behind a
package-private accessor for tests only.

### Step 4 — BY_METHOD isolation guard (RED → GREEN)

Add a paired test class with `@JudoTest(dataSourceMode = BY_METHOD)` and
identical structure asserting that `getInjector()` returns **different**
instances per method. This guards against the cache leaking into BY_METHOD.

### Step 5 — performance regression guard

Tighten the assertion in `RackinspectModelClassCachePerformanceTest`:

```java
assertTrue(cachedAvgNs * 5 < firstNs,
        "Cached BY_CLASS test average must be at least 5x faster than the cold first test");
```

This will only pass after Steps 2+3 are green.

### Step 6 — documentation

- Update `agent-docs/api-reference.md` describing the cached fast path.
- Add a short note in `agent-docs/TEST-CONFIGURATION.md` explaining the
  invariants users must keep (no schema mutation between methods etc.).

## 6. Risks / trade-offs

| Risk | Mitigation |
|---|---|
| **Stateful interceptor instance carries state across methods.** The Guice injector now holds the same interceptor singleton reused across methods. | Document that for stateful interceptors, users should switch to `BY_METHOD`. Reset interceptor state in `@AfterEach` if needed. Add a hook on `JudoRuntimeFixture` `resetInterceptors()` that re-runs `ReferenceInjector` to clear references. |
| **Schema-mutating tests would break** (e.g. a test that drops a table and expects re-migration in the next test). | Same as above — explicitly call out in docs. Such tests must use `BY_METHOD`. |
| **Custom modules with per-test state** built in `@JudoTest(modules=...)` are constructed once. | Document. Also, make the cache key include module classes so that two test classes with different `modules` get different injectors. (Already handled via cache key §4.1.) |
| **Singleton bleed across test classes when using `SINGLETON` mode with different `@JudoTest` configs.** Pre-existing issue from previous JNG-6374; this proposal makes it more visible. | Out of scope for this proposal but the cache key approach naturally fixes it: key by `(modelName, dialect, modelSource, modules, interceptors)` for the static map. |
| **JUnit `Store` cleanup at class end** must close the cached injector and shut down singletons (HikariCP, Liquibase). | Wrap `CachedRuntime` in a `Store.CloseableResource` — same pattern already used by `CloseableDatasourceFixture`. |
| **Backwards compatibility:** users who subclass `JudoRuntimeFixture` and override `init(...)` would not have their override called on the cached path. | Document. The fast path is opt-in via `BY_CLASS`/`SINGLETON` — `BY_METHOD` continues to call `init(...)` exactly as today. |

## 7. Out of scope

- Caching across **JVMs** (would require model serialization).
- Schema fingerprinting to safely re-run Liquibase only when needed.
- Sharing cached runtimes across test classes with identical configs (would
  bring SINGLETON behaviour to BY_CLASS — separate proposal).

## 8. Acceptance criteria

1. Existing tests in `judo-runtime-core-guice-testkit` continue to pass.
2. `ByClassRuntimeCacheTest` (Step 1) passes.
3. `ByClassLiquibaseSingleExecutionTest` (Step 3) passes.
4. `RackinspectModelClassCachePerformanceTest` shows ≥ 5× cached-avg / first-test
   speedup on the project's CI hardware.
5. `BY_METHOD` isolation test (Step 4) passes — separate injectors per method.
6. Build succeeds with `mvn clean install` (excluding pre-existing Docker tests).

## 9. Regression checks

The TDD tests in §5 also serve as **permanent regression guards** — they run on
every build and will catch any future change that silently breaks the cache.
This section enumerates them as a single check-list and adds the few extras
that are not natural "feature tests" but exist solely to detect regressions.

### 9.1 Correctness regressions

| # | Guard | What it catches | Frequency | Marker |
|---|---|---|---|---|
| R1 | `ByClassRuntimeCacheTest::injectorIsCached` (Step 1) | Someone refactored `JudoTestExtension` and the cache stopped applying. `assertSame` would flip to `assertNotSame`. | every build | unit |
| R2 | `ByClassRuntimeCacheTest::queryFactoryIsCached` (Step 1) | `QueryFactory` got accidentally rebuilt per method. | every build | unit |
| R3 | `ByClassRuntimeCacheTest::transactionManagerIsCached` (Step 1) | `PlatformTransactionManager` got rebuilt (often a symptom of a fresh injector). | every build | unit |
| R4 | `ByClassLiquibaseSingleExecutionTest` (Step 3) | Liquibase changelog re-validation was re-introduced for cached runs. Counter-based, exact-equal assertion. | every build | unit |
| R5 | `ByMethodIsolationTest` (Step 4) | The cache leaked into `BY_METHOD` (would break test isolation). Asserts `assertNotSame` on injectors. | every build | unit |
| R6 | `SingletonRuntimeCacheTest` (new — mirror of R1 for SINGLETON mode) | `SINGLETON` mode silently degraded to BY_METHOD-like behaviour. | every build | unit |
| R7 | `JudoTestFrameworkTest` (existing, unchanged) | Pre-existing baseline still works. | every build | unit |

### 9.2 Performance regressions

| # | Guard | What it catches | Frequency | Marker |
|---|---|---|---|---|
| R8 | `RackinspectModelClassCachePerformanceTest` cached-avg ≤ firstNs / 5 (Step 5) | Cache became ineffective (e.g. someone added per-test work that dwarfs the savings, or cache hit rate dropped). | nightly / on-demand | `@Tag("slow")` |
| R9 | Per-test absolute upper bound, e.g. `cachedAvgNs < SLOW_TEST_BUDGET_NS` (default 6 s) | Hardware-independent guard against new work creeping into the per-test path. | nightly | `@Tag("slow")` |

Both perf guards are intentionally **conservative** (5× vs theoretical ~8×;
6 s vs target ~3–5 s) so they do not flake on CI hardware variation while
still detecting a regression where the savings drop by more than half.

### 9.3 Cache-invariant regressions

These guards exist only to detect specific past mistakes from re-occurring:

| # | Guard | What it catches |
|---|---|---|
| R10 | `CacheKeyEqualityTest` | Two test classes with different `JudoTest#modules()` produce different cache keys. Catches a regression where someone over-aggressively reuses an injector across configs. |
| R11 | `CacheCloseableResourceTest` | After the test class finishes, the cached `Injector` and `HikariCP` pool are closed exactly once. Detects double-close (`IllegalStateException`) and leaks (still-open after class end). |
| R12 | `BackwardsCompatBuildModeTest` | Verifies the `BY_METHOD` code path still calls the legacy `JudoRuntimeFixture#init(...)` (so existing user subclasses that override `init` keep working). Uses a spy subclass. |

### 9.4 How regressions surface

- **PR builds:** R1–R7, R10–R12 run on every build (fast unit tests). Failure
  blocks merge. R8–R9 are skipped (tagged `slow`).
- **Nightly / `-Pslow-tests`:** R8–R9 run end-to-end against the rackinspect
  model. If they fail, an issue is filed automatically (CI hook out of scope
  here).
- **Local developer run:** `mvn test -Dtest='!*Performance*'` runs the
  correctness guards in seconds; full suite runs the perf guards too.

### 9.5 Maintenance rules

When modifying `JudoTestExtension` or `JudoRuntimeFixture`:

1. **Never weaken** any of R1–R12 to make a failing build green; investigate
   the cause first.
2. If the perf budget (R8/R9) genuinely needs adjustment because of an
   intentional change, update both the test threshold **and** this proposal in
   the same PR with justification.
3. Adding a new datasource mode requires a new R*-style test class for it.
4. Adding new cacheable artifacts requires extending `CachedRuntime` *and* a
   new `assertSame` test in R1–R3 style.

## 10. Implementation summary

Files to change (estimated lines):

| File | Lines |
|---|---|
| `JudoTestExtension.java` | ~+60 (cache lookup, key, store wrapper) |
| `JudoRuntimeFixture.java` | ~+25 (`prepareWithCachedRuntime`, package-private getters) |
| `agent-docs/TEST-CONFIGURATION.md` | ~+30 (caching invariants section) |
| `agent-docs/api-reference.md` | ~+15 |
| New test files (Steps 1, 3, 4) | ~+200 |

Total: ~+330, ~0 deletions. Net additive.

Additional regression-guard tests from §9.3 (R10–R12): ~+150 LOC across three
small test classes.
