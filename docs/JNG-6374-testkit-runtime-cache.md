# JNG-6374 — Testkit Runtime Cache: Branch Summary

> **Branch:** `feature/JNG-6374_cache_model_loader_in_testkit`
> **Module:** `judo-runtime-core-guice-testkit`
> **Status:** Implementation complete; full reactor green under Java 21; pending push/PR.
> **OpenSpec changes:** `cache-byclass-test-runtime`, `judo-test-enable-runtime-cache-flag`, `share-injector-opt-in` (final shape).
> **Final API:** `@JudoTest#shareInjector` (boolean, default `false`).

This document is the **canonical narrative** of every behavioural change introduced
by this branch vs `develop`, grouped by theme. Each section answers two questions:
**what** changed, and **why**. Code paths, types, and tests are named so this doc
can be used as a navigation index. The API names and defaults described here are
the **final shipping shape**; an intermediate `cacheRuntime` flag existed mid-branch
but was renamed and inverted before merge — see §17 "Evolution / Changelog".

For machine-checkable requirements, see the OpenSpec specs under
`openspec/changes/cache-byclass-test-runtime/specs/guice-testkit/spec.md`,
`openspec/changes/judo-test-enable-runtime-cache-flag/specs/guice-testkit/spec.md`,
and `openspec/changes/share-injector-opt-in/specs/guice-testkit/spec.md`.

---

## Commits on this branch (vs `develop`)

| # | Hash | Date | Subject |
|---|---|---|---|
| 1 | `e0bfe372` | 2026-02-02 | JNG-6374 Cache model loader in testkit for BY_CLASS and SINGLETON modes (foundation) |
| 2 | `52b642d8` | 2026-02-04 | Merge develop → branch |
| 3 | `950ff0bb` | 2026-05-05 | Merge develop → branch |
| 4 | `26141997` | 2026-05-06 | Cache full runtime artifacts ("caxhed model for class running") — major commit |
| 5 | `31c6097a` | 2026-05-08 | Add `cacheRuntime` opt-out flag (later renamed to `shareInjector` in #9) |
| 6 | `a73e1f6e` | 2026-05-11 | Add MDs + `ResolveDialectTest`, `RoutingPredicateTest` |
| 7 | `fe8c7f2d` | 2026-06-01 | Key SINGLETON caches by config + harden cached-runtime fixture (CodeRabbit follow-ups) |
| 8 | `a7b917c7` | 2026-06-01 | Rename `cacheRuntime` → `shareInjector` and invert default to `false` |
| 9 | `be8a613e` | 2026-06-01 | Mark commit task done in `share-injector-opt-in` tasks.md |
| 10 | `f935d5ff` | 2026-06-01 | Document `shareInjector = false` per-method overhead |
| 11 | `0aa7008a` | 2026-06-01 | Revert standalone migration guide (info kept in TEST-CONFIGURATION.md / troubleshooting.md) |

---

## 1. The problem (motivation)

`@JudoTest` already supported three datasource lifecycles:

| Mode | Datasource lifecycle | Runtime lifecycle (pre-branch) |
|---|---|---|
| `BY_METHOD`  | Fresh per method            | Fresh per method                 |
| `BY_CLASS`   | Shared across the class     | **Rebuilt per method**           |
| `SINGLETON`  | Shared across the JVM       | **Rebuilt per method**           |

The previous "cache" only memoised the `JudoModelLoader`. Empirical measurement on
a 20-method `BY_CLASS` test class with the real-world **rackinspect** model
(~20 MB ASM):

```
test count                       : 20
first test (cold)                : ~28 700 ms
cached test avg                  : ~26 000 ms
model-only cache speed-up        : ~1.10× (~10%)
```

Model load was only **~3 s** out of the **~28 s** per-method cost. The remaining
**~25 s** was *repeated* work that the inputs make safely cacheable:

1. `QueryFactory` extraction from the ASM model
2. Liquibase changelog re-validation (`init.execute(datasource)`)
3. `databaseModule` rebuild (`JudoHsqldbModule` / `JudoPostgresqlModule`)
4. **Full Guice `Injector` reconstruction** (largest single cost)
5. `PlatformTransactionManager` resolution

This branch caches all five at the appropriate scope, targeting cached per-test
cost of **3–5 s** (a **≥ 5×** speed-up), without changing the public `@JudoTest`
API.

---

## 2. What gets cached, and at which scope

### 2.1 BY_CLASS — one entry per `(class, config)` tuple

Lifecycle: created in `beforeAll`, closed in `afterAll`.

Stored in JUnit's `ExtensionContext.Store` under
`Namespace.create(JudoTestExtension.class, testClass)`.

**Cache key — `ByClassCacheKey` (record):**

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

**Why every field is in the key:**

| Field | Reason |
|---|---|
| `modelName`, `dialect`, `modelSource` | Different inputs ⇒ different loaded model. |
| `testClass` | Two classes with the same model must still not share an injector (different `@JudoTest` configs are possible). |
| `modules` | Guice modules change the injector graph. |
| `interceptors` | Different interceptors change runtime behaviour. |

Alternative *"key by `Class<?>` only"* was rejected: `@JudoTest` config can vary
across inheritance and nested classes (D1 in `design.md`).

### 2.2 SINGLETON — JVM-wide maps, keyed narrowly

Two **separate** maps, keyed by what actually determines each resource. Lifecycle:
created lazily; closed by a JVM-shutdown hook (`closeAllSingletonRuntimes()`).

| Resource | Map | Key | Type |
|---|---|---|---|
| `JudoModelLoader`         | `singletonModelLoaders`   | `SingletonModelKey(modelName, dialect, modelSource)`                    | `ConcurrentHashMap` |
| `CloseableDatasourceFixture` | `singletonDatasources` | `SingletonDatasourceKey(dialect, container)`                            | `ConcurrentHashMap` |
| `CachedRuntime` (injector + ancillaries) | `singletonRuntimes` | `ByClassCacheKey` (re-used; `testClass` field intentionally null/irrelevant for SINGLETON) | `ConcurrentHashMap` |

**Why the SINGLETON keys are *narrower* than `ByClassCacheKey`:**
the loaded model depends only on `(modelName, dialect, modelSource)` and the
physical database fixture depends only on `(dialect, container)`. Test class,
modules, and interceptors do not affect *what file gets loaded* or *which database
is started*. Keying narrowly maximises reuse, while keying the *runtime* by the
full `ByClassCacheKey` correctly disambiguates different module/interceptor sets.

### 2.3 The cached value — `CachedRuntime`

Package-private final record-like class wrapping the heavy artifacts:

```java
final class CachedRuntime implements ExtensionContext.Store.CloseableResource {
    final JudoModelLoader modelLoader;
    final Dialect dialect;
    final QueryFactory queryFactory;
    final RdbmsInit rdbmsInit;
    final Module databaseModule;
    final SimpleLiquibaseExecutor liquibaseExecutor;  // CountingLiquibaseExecutor at runtime
    final Injector injector;
    final PlatformTransactionManager transactionManager;
    final TestOperationCallInterceptorProvider interceptorProvider; // added in fe8c7f2d
    private final AtomicBoolean closed = new AtomicBoolean(false);
    public void close() { /* idempotent: closes Hikari pool, releases injector */ }
}
```

Implements JUnit's `CloseableResource` so the `Store` itself triggers `close()`
when the test class finishes (BY_CLASS) or when the singleton shutdown hook runs
(SINGLETON). `AtomicBoolean` guarantees one-and-only-once cleanup.

A backwards-compatible 8-arg constructor (without `interceptorProvider`) is
preserved so synthetic `CachedRuntime` instances built in unit tests continue
to compile.

### 2.4 BY_METHOD — unchanged

Every method continues to receive a fresh runtime (fresh datasource, fresh
injector). Isolation invariant preserved. No cache is consulted, no cache entry
created.

---

## 3. Routing — how `JudoTestExtension` decides

The decision between cold path (build everything) and fast path (reuse a
`CachedRuntime`) is now an *extractable*, *unit-testable* pure predicate in
`JudoTestExtensionRouting`:

```java
static boolean isClassScopedMode(JudoTest.DataSourceMode mode) {
    return mode == BY_CLASS || mode == SINGLETON;
}

static boolean useCache(boolean isClassLevel,
                        JudoTest.DataSourceMode mode,
                        boolean shareInjector) {
    if (!isClassLevel) return false;                 // method-level @JudoTest ⇒ cold path
    if (mode == BY_METHOD) return false;             // nothing class-scoped to cache
    return shareInjector;                            // BY_CLASS and SINGLETON honour the flag uniformly
}
```

Why extracted: `RoutingPredicateTest` and `ColdPathModelLoaderTest` can exercise
every branch with zero JUDO infrastructure, model loading, or database I/O.

Cold-path model-loader gate (also extracted, fe8c7f2d):

```java
JudoModelLoader coldPathModelLoader(boolean isClassLevel, ExtensionContext.Store store);
```

Returns a previously stored loader **only at class level**; method-level
invocations always rebuild. Prevents accidental cross-method reuse when a method
requests a different model configuration. Covered by `ColdPathModelLoaderTest`.

---

## 4. The fast path — `JudoRuntimeFixture#prepareWithCachedRuntime`

New entry point added to `JudoRuntimeFixture` that bypasses the trio
`initQueryFactory → initModules → Guice.createInjector` when a valid
`CachedRuntime` is provided:

```java
void prepareWithCachedRuntime(CachedRuntime cached, Object testInstance) {
    this.modelLoader          = cached.modelLoader;
    this.dialect              = cached.dialect;
    this.queryFactory         = cached.queryFactory;
    this.rdbmsInit            = cached.rdbmsInit;
    this.databaseModule       = cached.databaseModule;
    this.simpleLiquibaseExecutor = cached.liquibaseExecutor;
    this.injector             = cached.injector;
    this.transactionManager   = cached.transactionManager;
    this.interceptorProvider  = cached.interceptorProvider;   // fe8c7f2d
    if (testInstance != null) cached.injector.injectMembers(testInstance);
}
```

`BY_METHOD` and method-level `@JudoTest` continue to call the original
`init(...)` path — the cache is opt-in via routing.

### 4.1 Why we carry the `TestOperationCallInterceptorProvider` (fe8c7f2d)

Without this field, the fast-path fixture threw a misleading
`IllegalStateException("Interceptor provider not available. Call init() first.")`
even though the user never skipped init — the extension chose the fast path
on their behalf. The user-visible message blamed the test author for a decision
the framework made.

The provider is now captured during the cold path (when init *is* called) and
re-attached every time the cache is adopted. **Important caveat:** under
`BY_CLASS` / `SINGLETON`, the provider is class-shared mutable state. Runtime
mutations made by one test method are visible to sibling methods sharing the
same cache. Documented in-code and in `interceptor-testing.md`. Tests that need
isolation must either use `BY_METHOD` or call `@BeforeEach` to reset
interceptor state explicitly.

---

## 5. The `shareInjector` opt-in flag (a7b917c7)

### 5.1 What it is

A class-level element on `@JudoTest`, default `false`. Setting it to `true`
opts a `BY_CLASS` or `SINGLETON` test class into the cached fast path:

```java
@JudoTest(
    dataSourceMode = BY_CLASS,
    shareInjector = true   // opt into the cached fast path; same Injector across methods
)
```

With the default `false`, the class still benefits from the shared
`DataSource` and cached `JudoModelLoader`, but every test method receives a
fresh `Injector` + `QueryFactory` + interceptor instances — the same
behaviour an existing `@JudoTest(BY_CLASS)` class has on `develop` today.

### 5.2 Semantics (final)

| `dataSourceMode` | `shareInjector` | Behaviour |
|---|---|---|
| `BY_METHOD`  | *(ignored)*           | Everything fresh per method |
| `BY_CLASS`   | `false` *(default)*   | DataSource + model cached per class; Injector + interceptors fresh per method |
| `BY_CLASS`   | `true`                | Full cached fast path — same Injector across methods (~5×–10× perf win) |
| `SINGLETON`  | `false` *(default)*   | DataSource + model cached JVM-wide; Injector + interceptors fresh per method |
| `SINGLETON`  | `true`                | Full cached fast path — same Injector across the JVM |
| Method-level `@JudoTest` | *(any)* | Ignored; always cold path |

### 5.3 Why SINGLETON honours the flag uniformly with BY_CLASS

An earlier intermediate design (the `cacheRuntime` flag) had SINGLETON
*ignore* the flag on the grounds that disabling the cache on a JVM-wide
DataSource would re-run Liquibase against the shared database per method.
That concern was re-analysed before shipping and discarded:

1. `DATABASECHANGELOG` records every applied changeset. Re-invocation is a
   no-op for already-applied changesets, not a re-application.
2. `DATABASECHANGELOGLOCK` serialises concurrent invocations against the
   same physical schema. No "two workers race to apply" scenario exists.
3. The actual cost is one extra `DATABASECHANGELOG` round-trip per method
   plus `Injector` reconstruction — **performance, not correctness**.

So `SINGLETON + shareInjector = false` is a valid, safe configuration that
simply costs more per method than `SINGLETON + shareInjector = true`.
The two modes now route identically; the predicate has no per-mode special
case (see §3).

### 5.4 Method-level annotations are ignored

`shareInjector` is meaningful only on class-level `@JudoTest`. The routing
predicate's `isClassLevel` guard skips the cache entirely for method-level
annotations; the flag value is never read in that branch. Covered by
`MethodLevelShareInjectorIgnoredTest`.

---

## 6. The SINGLETON keying regression (fe8c7f2d, the biggest correctness fix)

### 6.1 The bug

Pre-fix code held the JVM-wide SINGLETON resources in **unkeyed `volatile`
fields**:

```java
private static volatile JudoModelLoader        singletonModelLoader;
private static volatile CloseableDatasourceFixture singletonDatasource;
```

Both were write-once. The first SINGLETON-annotated test class to run won —
every subsequent SINGLETON class silently received those instances, even if
its `@JudoTest` resolved to a different model name, dialect, or container.

Failure modes:

- Class A `@JudoTest(modelName="x", dialect="hsqldb")` runs first.
- Class B `@JudoTest(modelName="y", dialect="postgresql")` runs second.
- Class B got class A's hsqldb fixture and class A's "x" model loader.
- Tests in B then either ran against the wrong schema (silent data confusion)
  or threw obscure type-resolution errors.

### 6.2 The fix

Replace each `volatile` field with a `ConcurrentHashMap` keyed by the value
type whose equality matches the resource's identity:

```java
private static final ConcurrentHashMap<SingletonModelKey, JudoModelLoader>
        singletonModelLoaders = new ConcurrentHashMap<>();

private static final ConcurrentHashMap<SingletonDatasourceKey, CloseableDatasourceFixture>
        singletonDatasources = new ConcurrentHashMap<>();
```

Both lookups now use double-checked / `computeIfAbsent`-style initialisation
so concurrent test JVMs do not race.

### 6.3 New value types

```java
final class SingletonDatasourceKey {     // (dialect, container)
    private final String dialect;
    private final String container;
    // structural equals/hashCode
}

final class SingletonModelKey {          // (modelName, dialect, modelSource)
    private final String modelName;
    private final String dialect;
    private final JudoTest.ModelSource modelSource;
    // structural equals/hashCode
}
```

Both are package-private (internal testkit seams) and named after the resource
they identify — different from `ByClassCacheKey` precisely because the keying
contract is narrower (see §2.2).

### 6.4 Helpers consolidated (DRY)

- `resolveContainerName(JudoTest annotation, String dialect)` — used both when
  computing the cache key and inside `createDatasourceFixture`. Eliminates a
  source of key/value drift.
- `registerSingletonShutdownHook(ExtensionContext)` — one call site for the
  JVM-shutdown registration, gated by a single `singletonShutdownRegistered`
  flag. Replaces the prior per-runtime registration sprinkled across creation
  sites.
- `closeAllSingletonRuntimes()` — now closes *every* entry in all three
  SINGLETON maps and clears them, leaving zero leaked state for the next
  forked test JVM.

### 6.5 Visibility relaxations for test access

- `CloseableDatasourceFixture` — now package-private (same package as the
  functional tests).
- `singletonModelLoadersForTesting()`, `singletonDatasourcesForTesting()` —
  package-private accessors that expose the live maps to white-box regression
  tests. Not part of any public API.

### 6.6 Tests added

- `SingletonModelLoaderKeyingTest` — equal `SingletonModelKey`s share a
  loader; distinct keys (vary `modelName`, `dialect`, or `modelSource`) map
  to distinct entries; shutdown clears the map.
- `SingletonDatasourceKeyingTest` — equal `SingletonDatasourceKey`s share a
  fixture; distinct keys map to distinct entries; `closeAllSingletonRuntimes()`
  invokes `teardownDatasource()` on every entry exactly once.

---

## 7. `CountingLiquibaseExecutor` correctness (fe8c7f2d)

### 7.1 The bug

Pre-fix:

```java
@Override public void createDatabase(DataSource ds, LiquibaseModel m) {
    executionCount.incrementAndGet();    // count BEFORE the work
    super.createDatabase(ds, m);
}
```

A failed first attempt (e.g. transient DB connect error) followed by a retry
incremented the counter twice. The cache regression tests interpret a count
> 1 as "Liquibase ran twice on the cached runtime — cache is broken." The
counter was producing a false positive on legitimate retries.

### 7.2 The fix

```java
@Override public void createDatabase(DataSource ds, LiquibaseModel m) {
    super.createDatabase(ds, m);         // run the work
    executionCount.incrementAndGet();    // count ONLY on success
}
```

Javadoc clarified: counts *successful* completions.

### 7.3 Visibility

`final class` → `class`. Test fixtures (notably the retry-injection scenarios
in `CountingLiquibaseExecutorTest`) need to subclass for fault injection.

### 7.4 Test

`CountingLiquibaseExecutorTest` now includes a failed-then-retry scenario
asserting count == 1 after one successful completion.

---

## 8. New & updated tests — full inventory

### Production-code units (`src/main/java/.../fixture/`)
| File | Status |
|---|---|
| `JudoTestExtension.java` | modified — keyed SINGLETON maps, routing extracted, cold-path gate |
| `JudoRuntimeFixture.java` | modified — `prepareWithCachedRuntime`, interceptor carry-over |
| `CachedRuntime.java` | modified — added `interceptorProvider`; backwards-compat constructor |
| `CountingLiquibaseExecutor.java` | modified — count-after-success; non-final |
| `JudoTest.java` | modified — added `shareInjector` flag (default `false`); `ModelSource` enum already existed |
| `JudoTestExtensionRouting.java` | new — pure routing predicates |
| `SingletonDatasourceKey.java` | new — `(dialect, container)` key |
| `SingletonModelKey.java` | new — `(modelName, dialect, modelSource)` key |
| `ByClassCacheKey` (record) | new — full BY_CLASS / SINGLETON-runtime key |

### Tests (`src/test/java/.../fixture/`)
| File | Purpose |
|---|---|
| `CacheKeyEqualityTest` | `ByClassCacheKey` / Singleton key equality contracts |
| `CachedRuntimeCloseableResourceTest` | `close()` idempotency, Hikari pool released |
| `SingletonRuntimeMapTest` | SINGLETON runtime map basics |
| `CountingLiquibaseExecutorTest` | Count-after-success; failed-retry scenario |
| `PrepareWithCachedRuntimeTest` | Fast path adopts cached fields, including interceptor provider |
| `ShareInjectorFlagTest` | `shareInjector` default is `false`; routing predicate consumes both mode and flag; legacy `cacheRuntime` element absent |
| `DefaultIsFreshRuntimePerMethodTest` | The headline contract — default annotation yields a fresh runtime per method |
| `SingletonShareInjectorFlagTest` | SINGLETON honours the flag uniformly with BY_CLASS; truth-table parity |
| `MethodLevelShareInjectorIgnoredTest` | Method-level `shareInjector=true` does NOT enable caching |
| `RoutingPredicateTest` | Every branch of `useCache(...)` incl. uniform-truth-table sanity check |
| `ResolveDialectTest` | `resolveContainerName(...)` correctness |
| `SingletonDatasourceKeyingTest` | Two SINGLETON classes w/ different dialects ⇒ distinct fixtures |
| `SingletonModelLoaderKeyingTest` | Distinct `(modelName, dialect, modelSource)` ⇒ distinct loaders |
| `ColdPathModelLoaderTest` | Class-level reuses; method-level ignores; empty store ⇒ null |
| `RackinspectModelClassCachePerformanceTest` *(downstream consumer)* | `@Tag("slow")` — 20-method BY_CLASS perf guard; requires `shareInjector = true` to demonstrate the cache |

---

## 9. Documentation updated

### Top-level
- `judo-runtime-core-guice-testkit/README.md`
- `judo-runtime-core-guice-testkit/TEST-CONFIGURATION.md` (new file)
- `docs/proposals/JNG-XXXX-by-class-runtime-caching.md` (the original proposal)
- **`docs/JNG-6374-testkit-runtime-cache.md` (this file)**

### `judo-runtime-core-guice-testkit/agent-docs/` (kept in sync with top-level)
- `README.md` — overview pointer to TEST-CONFIGURATION.md
- `TEST-CONFIGURATION.md` — "Behavioural sharing" section: stateful
  interceptors, schema-mutating tests, the `BY_METHOD` escape hatch, the
  five-row `shareInjector` behaviour matrix, plus a "Per-method overhead
  under `shareInjector = false`" sub-section with cost breakdown.
- `api-reference.md` — `@JudoTest` attributes (incl. `shareInjector`),
  `prepareWithCachedRuntime` entry point, `CachedRuntime` lifecycle.
- `interceptor-testing.md` — stateful-interceptor callout: the default
  (`shareInjector = false`) already gives fresh interceptors per method;
  `@BeforeEach` reset only needed when `shareInjector = true`.
- `troubleshooting.md` — entries covering: custom `init(...)` overrides,
  "Why are my BY_CLASS tests slower after the upgrade?" → add
  `shareInjector = true`, "Can I use SINGLETON without sharing the
  injector?" → yes, default, and "How much overhead does
  `shareInjector = false` add?" with rule-of-thumb numbers.

---

## 10. OpenSpec changes

### `openspec/changes/cache-byclass-test-runtime/`

Primary change for this branch. Requirements:

1. Class-Level Runtime Caching for `BY_CLASS` Mode — same `Injector` /
   `QueryFactory` / `PlatformTransactionManager` per class.
2. JVM-Level Runtime Caching for `SINGLETON` Mode — identical-config
   SINGLETON classes share the runtime.
3. Liquibase Executes Exactly Once Per Cached Runtime.
4. Cached Runtime Resources Are Closed Exactly Once.
5. `BY_METHOD` Mode Preserves Per-Test Isolation.
6. Cache Key Includes Modules and Interceptors.
7. **SINGLETON Model Loader And Datasource Are Keyed By Configuration**
   (added fe8c7f2d).
8. Performance Speed-up Guard for `BY_CLASS` Mode (`@Tag("slow")`,
   `firstNs / cachedAvgNs ≥ 5`, `cachedAvgNs < 6 s`).

`tasks.md` numbered through section 10 — sections 8 (CodeRabbit 1st-pass
keying fix) and 9 (CodeRabbit 2nd-pass: cold-path gate, interceptor
provider, Liquibase count timing, etc.) document the post-implementation
hardening.

### `openspec/changes/judo-test-enable-runtime-cache-flag/`

Intermediate change covering the original `cacheRuntime` flag. Superseded
by `share-injector-opt-in` (the rename + invert + uniform-SINGLETON change
shipped in a7b917c7). Retained as historical record. The directory
contains both the original `proposal.md` and the v2 simplification
`proposal-v2-simplify-cacheRuntime.md`.

### `openspec/changes/share-injector-opt-in/`

The final-shape change. Renames `cacheRuntime` → `shareInjector`, flips the
default to `false`, and unifies SINGLETON with BY_CLASS routing. Spec
delta uses 5 REMOVED, 3 MODIFIED, and 5 ADDED requirements to refine the
requirements from the two prior changes into the shipping shape. See
`design.md` decisions D1–D7 (especially D2 "Default is opt-in for safety"
and D3 "SINGLETON honours the flag" with the Liquibase re-analysis).

---

## 11. Performance result

| Metric | Value | Source |
|---|---|---|
| Cold-method elapsed (rackinspect, 20-method `BY_CLASS`) | ~28 700 ms | `design.md` |
| Cached-method avg (model-only cache, pre-branch) | ~26 000 ms | `design.md` |
| Cached-method avg (full runtime cache, target) | **3–5 s** | `proposal.md` |
| Enforced ratio `firstNs / cachedAvgNs` | **≥ 5** | spec.md requirement |
| Enforced absolute `cachedAvgNs` ceiling | **< 6 s** | spec.md requirement |
| Effective speed-up at target | ≈ 5×–10× per cached method | derivation |

Caveats:

- The 5× floor is the **enforced contract**, not the observed maximum.
- The slow perf test is `@Tag("slow")`; default `mvn test` does not run it.
- Tasks 6.2 and 6.3 (re-run on CI hardware, record numbers in PR
  description) are **still unchecked** — the contract is enforced by the
  assertion in `RackinspectModelClassCachePerformanceTest`, but the
  branch-final numbers are not yet recorded.

---

## 12. Backwards compatibility

| Surface | Change | Compat impact |
|---|---|---|
| `@JudoTest` annotation | `shareInjector` element added, default `false` | None — the default preserves existing test behaviour (fresh Injector per method on BY_CLASS / SINGLETON). |
| `DataSourceMode` enum | unchanged | None. |
| `JudoRuntimeFixture` public API | `prepareWithCachedRuntime` added; existing methods unchanged | None. |
| `JudoTestExtension` | internal routing change; same JUnit5 SPI | None. |
| `CachedRuntime` constructor | added 9-arg ctor; preserved 8-arg ctor | None — existing test fixtures compile. |
| `CountingLiquibaseExecutor` visibility | `final class` → `class` | Strictly relaxing — no caller breaks. |
| `CloseableDatasourceFixture` visibility | tightened to package-private | Internal testkit type; no public callers. |

No public API removed, no annotation element from any *released* version
renamed (`cacheRuntime` was an unreleased intermediate name), no default
behaviour changed unless the test author explicitly opts in via
`shareInjector = true`. The intermediate `cacheRuntime` flag never shipped
in a tagged release; downstream cost of the inversion is zero.

---

## 13. Risks and known invariants for test authors

Documented in `agent-docs/TEST-CONFIGURATION.md` § *Caching invariants*:

1. **Schema-mutating tests** (DDL beyond the changelog) — the default
   (`shareInjector = false`) is the right choice: Liquibase re-runs each
   method, idempotently no-op'ing on changesets already applied via
   `DATABASECHANGELOG`. If you set `shareInjector = true`, your in-test
   schema changes will not be re-applied for sibling methods.
2. **Stateful interceptors** — the default already gives fresh interceptor
   instances per method. Only when `shareInjector = true` do interceptors
   share state across methods; reset in `@BeforeEach` if you want both
   sharing (for perf) and clean state.
3. **Different `@JudoTest` configs on two `SINGLETON` classes** are safe
   *only* because of the new SINGLETON keying (§6). Pre-fe8c7f2d branches
   silently shared the first class's resources — do not back-port without
   the keying fix.
4. **`@BeforeAll`-captured `Injector` references** under `shareInjector =
   false` go stale after the first method (each method gets a fresh
   `Injector`). Resolve via `fixture.getInjector()` inside each `@Test`
   instead of caching, **or** set `shareInjector = true` if you want a
   stable reference across methods.

---

## 14. Verification matrix

| Check | Status |
|---|---|
| `mvn test` (full reactor under Java 21, excl. Docker-only `JudoDefaultPostgresqlModuleTest`) | **BUILD SUCCESS** — 282 tests, 0 failures, 0 errors, 25 skipped, 44.7s |
| `mvn test` in `judo-runtime-core-guice-testkit` alone | 190 tests, 0 failures, 25 pre-existing skipped |
| `openspec validate cache-byclass-test-runtime` | passes |
| `openspec validate judo-test-enable-runtime-cache-flag` | passes |
| `openspec validate share-injector-opt-in` | passes |
| Perf guard `RackinspectModelClassCachePerformanceTest -Dgroups=slow` on CI hardware | **pending** (lives in downstream `rackinspect` consumer; needs `shareInjector = true` to demonstrate the cache) |
| PR opened + reviewers requested | pending |
| Archive after merge | pending |

---

## 15. File-level diff vs `develop` (final)

```
86 files changed, ~6,600 insertions(+), ~240 deletions(-)
```

Concentrated in:
- `judo-runtime-core-guice-testkit/src/main/java/.../fixture/` — production code
- `judo-runtime-core-guice-testkit/src/test/java/.../fixture/` — tests
- `judo-runtime-core-guice-testkit/agent-docs/`, top-level `README.md`,
  `TEST-CONFIGURATION.md`
- `openspec/changes/cache-byclass-test-runtime/`
- `openspec/changes/judo-test-enable-runtime-cache-flag/`
- `.pi/skills/openspec-*/`, `.pi/prompts/opsx-*` — tooling/skills (not
  feature code)

No production code outside `judo-runtime-core-guice-testkit` was modified.

---

## 16. Glossary

| Term | Meaning here |
|---|---|
| **Cold path** | `JudoRuntimeFixture#init(...)` — builds `QueryFactory`, Liquibase exec, modules, injector from scratch. |
| **Fast path** | `JudoRuntimeFixture#prepareWithCachedRuntime(...)` — adopts a `CachedRuntime`'s fields. |
| **BY_CLASS** | Test datasource and (now) runtime live for one test class. |
| **SINGLETON** | Test datasource and runtime live JVM-wide. |
| **BY_METHOD** | Everything rebuilt per method (no cache). |
| **`shareInjector`** | Class-level `@JudoTest` flag (default `false`) — opt into the cached fast path for BY_CLASS / SINGLETON. Ignored for BY_METHOD and method-level annotations. Supersedes the legacy `cacheRuntime` flag. |
| **`cacheRuntime`** | **Removed in `share-injector-opt-in`.** Was a default-`true` opt-out for BY_CLASS only with a SINGLETON carve-out. Replaced by the inverted, uniform `shareInjector`. |
| **`ByClassCacheKey`** | Full key for cached runtimes — includes test class, modules, interceptors. |
| **`SingletonModelKey`** | Narrow key for the SINGLETON model-loader map. |
| **`SingletonDatasourceKey`** | Narrow key for the SINGLETON datasource map. |
| **CodeRabbit** | The automated PR review service whose findings drove the fe8c7f2d hardening pass. |

---

## 17. Evolution / Changelog

The public API name and default of the sharing flag evolved twice on this
branch before it shipped. The doc above describes the **final shape**; this
section records the intermediate steps for archaeology purposes only — no
tagged release ever exposed the intermediate names.

| Phase | Element name | Default | SINGLETON behaviour | Status |
|---|---|---|---|---|
| v1 (`31c6097a`) | `cacheRuntime` | `true` | flag honoured | abandoned |
| v2 (resolved in `fe8c7f2d`) | `cacheRuntime` | `true` | flag ignored (always cached) | abandoned |
| Final (`a7b917c7`, change `share-injector-opt-in`) | **`shareInjector`** | **`false`** | **flag honoured uniformly** | **shipped** |

The v1 → v2 step removed a Liquibase-against-shared-DB concern by making
SINGLETON ignore the flag. The v2 → final step re-analysed that concern
(Liquibase is idempotent via `DATABASECHANGELOG` + serialised via
`DATABASECHANGELOGLOCK`), discarded it, and inverted the default to
follow the "isolation by default, sharing on opt-in" convention of
pytest, JUnit 5, and Testcontainers.

For full rationale, see `openspec/changes/share-injector-opt-in/design.md`
(decisions D1–D7, especially D2 and D3).
