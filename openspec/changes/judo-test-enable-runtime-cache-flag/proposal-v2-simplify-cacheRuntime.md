# Proposal v2: Simplify `cacheRuntime` — restrict to BY_CLASS, rename to `cacheInjector`

## Problem

The current `cacheRuntime` flag allows 5 mode/flag combinations, but two of
them are questionable and one is actively dangerous:

| Combo | Assessment |
|-------|------------|
| `BY_METHOD` + any | ✅ Flag ignored — fine |
| `BY_CLASS` + `true` | ✅ Happy path — big perf win |
| `BY_CLASS` + `false` | ⚠️ Niche — only real use case is custom `init()` overrides |
| `SINGLETON` + `true` | ✅ Maximum sharing — fine |
| **`SINGLETON` + `false`** | **🔴 Footgun — no valid use case, real risks** |

### Why `SINGLETON + false` is dangerous

1. **Liquibase race condition.** SINGLETON shares one DataSource JVM-wide.
   With `cacheRuntime = false`, every test method re-runs Liquibase against
   that shared database. If JUnit runs classes in parallel, multiple
   Liquibase instances hit the same schema concurrently → lock contention,
   `LockException`, or corrupted `DATABASECHANGELOGLOCK`.

2. **No real use case.** The three motivating scenarios for `cacheRuntime = false` are:
   - **Stateful interceptors** → just call `spy.reset()` in `@BeforeEach` (simpler, faster)
   - **Schema mutations** → you need a fresh DataSource too → use `BY_METHOD`
   - **Custom `init()` override** → only makes sense per-class, not JVM-wide

   Nobody wants "JVM-wide DataSource sharing but per-method Injector rebuilds
   across unrelated test classes." If you need per-method isolation, you don't
   want a JVM-wide DataSource.

3. **Misleading name.** `cacheRuntime` suggests "don't cache the runtime" but
   the DataSource and ModelLoader are *still* cached. Users reading
   `cacheRuntime = false` will expect full per-method freshness and be
   surprised when they share state via the DataSource.

### Why `BY_CLASS + false` is marginal

The three motivating scenarios each have simpler solutions:

| Scenario | `cacheRuntime = false` | Simpler alternative |
|----------|----------------------|---------------------|
| Stateful interceptors | Rebuilds entire Injector + Liquibase | `spy.reset()` in `@BeforeEach` (1 line) |
| Schema mutations | Re-runs Liquibase per method | `BY_METHOD` (correct isolation) |
| Custom `init()` override | Works | No simpler alternative — **this is the real use case** |

## Options

### Option A: Restrict `SINGLETON + false` (recommended)

**Change:** When `dataSourceMode = SINGLETON` and `cacheRuntime = false`,
throw `IllegalStateException` at test startup with a clear message:

```
@JudoTest(dataSourceMode = SINGLETON, cacheRuntime = false) is not supported.
SINGLETON mode shares a JVM-wide DataSource; disabling the runtime cache would
re-run Liquibase per method against the shared database, risking schema
corruption. Use dataSourceMode = BY_CLASS with cacheRuntime = false, or
dataSourceMode = BY_METHOD for full per-method isolation.
```

**Keep `BY_CLASS + false`** — it's the only combo with a legitimate use case
(custom `init()` overrides) and no concurrency risks.

**Impact:**
- One guard clause in `JudoTestExtension.beforeEach` or `JudoTestExtensionRouting`
- Update routing tests to expect the exception
- Update behavior matrix in docs (4 rows instead of 5)
- Backwards-compatible: nobody is using `SINGLETON + false` yet (the flag is unreleased)

### Option B: Rename to `cacheInjector`

Rename `cacheRuntime` → `cacheInjector` to precisely describe what it controls:

| Name | What it implies | What it actually does |
|------|----------------|----------------------|
| `cacheRuntime` | "Don't cache the runtime" | Only skips Injector/QueryFactory/Liquibase/TxManager |
| `cacheInjector` | "Don't cache the Injector" | Exactly that — plus the derived artifacts |

**Trade-off:** More precise naming, but `cacheInjector` doesn't mention
Liquibase re-runs (a side effect of rebuilding the Injector). Could use
`rebuildInjectorPerMethod` but that's verbose.

### Option C: Option A + Option B (recommended combination)

1. Rename `cacheRuntime` → `cacheInjector`
2. Restrict: throw on `SINGLETON + false`
3. Result: 3 valid combinations, all with clear semantics

| `dataSourceMode` | `cacheInjector` | Behavior |
|---|---|---|
| `BY_METHOD` | *(ignored)* | Everything fresh per method |
| `BY_CLASS` | `true` (default) | Everything cached per class |
| `BY_CLASS` | `false` | DataSource + model per class; Injector + Liquibase per method |
| `SINGLETON` | `true` (default) | Everything cached JVM-wide |
| `SINGLETON` | `false` | ❌ `IllegalStateException` |

### Option D: Remove `cacheRuntime` entirely

The only real use case (custom `init()` override) is very niche. Instead:
- Document `BY_METHOD` as the escape hatch
- Document `@BeforeEach` reset for stateful interceptors
- Remove the flag, the routing class, and the tests

**Trade-off:** Simplest codebase, but closes the door on the `init()` override
use case. Users with custom `init()` must use `BY_METHOD` and accept the
full cold cost (~28s per method).

## Recommendation

**Option A** (restrict `SINGLETON + false`) is the minimum viable fix —
small change, removes the real risk, keeps the legitimate use case.

**Option C** (A + rename) is the cleanest result but touches more surface
area. Worth it if we're confident nobody has started using `cacheRuntime`
externally (the flag is unreleased, so this is safe).

**Option D** is worth considering if we don't have concrete users needing
the `init()` override escape hatch today. YAGNI applies.

## Questions for review

1. Do we have any test classes today that override `JudoRuntimeFixture#init()`?
   If not, Option D (remove entirely) may be the right call.
2. Is the rename from `cacheRuntime` to `cacheInjector` worth the churn?
3. Should we log a warning (instead of throwing) for `SINGLETON + false` to
   be lenient during a transition period?
