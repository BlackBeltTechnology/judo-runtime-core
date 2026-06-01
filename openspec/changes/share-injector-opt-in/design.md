# Design: `shareInjector` opt-in

## Context

`cache-byclass-test-runtime` introduced a runtime cache for `BY_CLASS` and
`SINGLETON` modes, controlled implicitly by the mode selector.
`judo-test-enable-runtime-cache-flag` added an explicit `cacheRuntime` flag
defaulting to `true`, with a v2 carve-out making `SINGLETON` ignore the flag
on the grounds that disabling the cache against a JVM-wide datasource would
re-run Liquibase against a shared database and risk schema corruption under
parallel execution.

This change is a pre-ship refinement of that design. It exists because the
existing shape conflates two orthogonal concerns:

```
  Concern A: Resource lifecycle (how long does the datasource live?)
             → expressed by dataSourceMode

  Concern B: Behavioural sharing (do two test methods see the same Guice graph?)
             → currently coupled to dataSourceMode, with a partial escape hatch
```

The branch hasn't shipped a tagged release; the cost of fixing the conflation
*now* is one rename + one default flip + one removed special case. The cost
of shipping the conflation and fixing it later is a breaking API change.

## Goals / Non-Goals

### Goals

- Make the two axes orthogonal in the annotation surface.
- Make both axes default to the safest value (isolation).
- Eliminate the `SINGLETON` carve-out — every mode honours the same flag
  uniformly.
- Use a flag name that reads as a positive opt-in.
- Preserve the 5×–10× perf win for users who explicitly request it.

### Non-Goals

- Changing `dataSourceMode` enum values or semantics.
- Changing `BY_METHOD` behaviour in any way.
- Changing the `ByClassCacheKey` / `SingletonModelKey` /
  `SingletonDatasourceKey` keying contract from `cache-byclass-test-runtime`.
- Changing the closeable-resource lifecycle of `CachedRuntime`.
- Re-running Liquibase analysis from scratch — see D2 below for the
  specific re-evaluation.

## Decisions

### D1. Rename `cacheRuntime` → `shareInjector`

The new name describes **what the user sees**, not **what the framework does
internally**. The mental model the annotation should reinforce is:

> *"Do two test methods receive the same Guice `Injector` instance?"*

`cacheRuntime` is framework-implementation language. `shareInjector` is
user-facing semantics. The cached `QueryFactory`, `PlatformTransactionManager`,
and `SimpleLiquibaseExecutor` are byproducts of sharing the injector that
holds them — describing the flag by its most visible consequence is the right
abstraction for the test author.

Alternative considered: `cacheInjector`. Rejected — "cache" still emphasises
the framework mechanism; "share" emphasises the user-visible behaviour.

### D2. Default value is `false` (isolation by default)

This is the philosophical core. Justified by:

- **Surprise minimisation.** The current default-`true` shape means a test
  author selecting `BY_CLASS` for *datasource* reasons silently inherits
  shared-injector and shared-interceptor state, with mutations leaking
  between methods. Inverting the default closes this footgun for everyone
  who didn't read the "Caching invariants" doc.
- **Positive opt-in reads cleanly.** `shareInjector = true` is an
  assertion of intent. `cacheRuntime = false` was a denial of a default.
- **Industry alignment.** pytest fixtures default to function-scoped;
  JUnit 5 defaults to `PER_METHOD`; Testcontainers defaults to per-test.
  The exception that proves the rule (Spring Test Context's aggressive
  ApplicationContext caching with `@DirtiesContext` opt-out) is widely
  considered Spring's most footgun-prone testing decision.
- **Zero downstream cost.** The previous flag is unreleased; nobody
  depends on the current default in published code.

The price: new BY_CLASS tests don't get the 5× perf win for free.
Mitigation: one keyword per class buys it back, and the docs lead with
that opt-in for users who care about perf. We trade a per-class typing
cost for an action-at-a-distance bug class.

### D3. `SINGLETON` honours the flag uniformly (reversing v2)

The v2 carve-out made `SINGLETON` ignore the flag on the grounds that
`SINGLETON + cacheRuntime = false` would re-run Liquibase against a shared
database under parallel execution, risking "schema corruption". This change
re-opens that conclusion and resolves it the other way:

**Re-evaluation of the Liquibase concern:**

1. **Idempotence via `DATABASECHANGELOG`.** Liquibase records every applied
   changeset in the `DATABASECHANGELOG` table. The second invocation against
   the same physical schema reads the table, sees every changeset already
   applied, and emits a no-op. Re-running Liquibase against an
   already-migrated DB is therefore correct, not corrupting.

2. **Concurrency safety via `DATABASECHANGELOGLOCK`.** Liquibase serialises
   concurrent invocations against the same DB using the changelog-lock
   table. Two test JVMs (or two parallel test methods) racing to call
   `init.execute(datasource)` will serialise on this lock, then both
   no-op on the changelog check. No "race to apply the changelog"
   scenario exists.

3. **The actual cost is performance, not correctness.** `SINGLETON +
   shareInjector = false` does pay for one extra `DATABASECHANGELOG`
   round-trip per method (plus injector reconstruction). Under parallel
   execution it may pay for lock contention on `DATABASECHANGELOGLOCK`.
   These are perf taxes, not correctness hazards.

4. **The Liquibase counting wrapper still serves its purpose.**
   `CountingLiquibaseExecutor#executionCount()` continues to assert
   "exactly one execution per cached runtime" *when* `shareInjector =
   true`. When `shareInjector = false` on `SINGLETON`, the equivalent
   assertion shifts to "exactly N executions for N methods, each
   completing successfully (idempotent no-ops after the first)". The
   counter mechanism is unchanged; only the test assertions change.

Therefore `SINGLETON + shareInjector = false` is a valid, safe configuration
that simply costs more per method than `SINGLETON + shareInjector = true`.
Making it impossible (the v2 stance) was solving a correctness problem that
does not exist.

The benefit of allowing it: API consistency. `shareInjector` becomes
truly orthogonal to `dataSourceMode`. Every combination has predictable,
documented semantics. There is no hidden special case.

### D4. Routing collapses to a single rule

`JudoTestExtensionRouting.useCache(...)` simplifies from:

```java
static boolean useCache(boolean isClassLevel, DataSourceMode mode, boolean cacheRuntime) {
    if (!isClassLevel) return false;
    if (mode == SINGLETON) return true;                    // ← v2 carve-out
    return mode == BY_CLASS && cacheRuntime;
}
```

to:

```java
static boolean useCache(boolean isClassLevel, DataSourceMode mode, boolean shareInjector) {
    if (!isClassLevel) return false;
    if (mode == BY_METHOD) return false;
    return shareInjector;                                  // uniform
}
```

The new shape removes a branch, removes the only mode-dependent special
case, and makes the predicate testable as a near-tautology against
`shareInjector`. `RoutingPredicateTest` shrinks accordingly.

### D5. Cache keys are unchanged

`ByClassCacheKey`, `SingletonModelKey`, and `SingletonDatasourceKey` continue
to identify cached resources the same way they did under
`cache-byclass-test-runtime`. The flag determines **whether** a lookup
happens, not **how** entries are keyed.

Consequence: a `SINGLETON` class with `shareInjector = false` still hits the
same `singletonDatasources` map for its datasource (datasources are still
shared per mode — that's what the mode means). It just does not consult
`singletonRuntimes` for its injector. The two maps were already independent
in `cache-byclass-test-runtime`; this change merely exercises that
independence.

### D6. Docs reorganisation — warnings move with the flag

Today's `TEST-CONFIGURATION.md § Caching invariants` warns about stateful
interceptors, schema-mutating tests, and the `BY_METHOD` escape hatch *under
the BY_CLASS section*. The implicit message: "if you choose BY_CLASS, accept
these constraints."

After this change, the same warnings move under the `shareInjector = true`
description. The new message: "if you opt into sharing, accept these
constraints." This matches the new mental model: BY_CLASS by itself is safe;
sharing is the source of subtle behaviour.

This is a cosmetic doc reshuffle, but it inverts the reader's experience:
the safe path is now the unmarked one, and the dangerous opt-in is the one
with warnings. That's the right shape for a framework that aims to be
boring-by-default.

### D7. The `since` tag

`@JudoTest#cacheRuntime` was tagged `@since 1.0.7` but never released. The
new `shareInjector` element takes the same `@since 1.0.7` tag (or whichever
version this change ships in). No `@Deprecated` shim is needed because the
old name has no callers.

## Risks / Trade-offs

| Risk | Mitigation |
|---|---|
| New users get slower default tests than the previous shape promised. | Doc the opt-in prominently in `README.md` and `TEST-CONFIGURATION.md`; show the 5× number next to the `shareInjector = true` example. |
| `SINGLETON + shareInjector = false` is slower than the v2 design implied. | Documented as the explicit cost of the orthogonality. Users who want max perf write `shareInjector = true`. |
| Liquibase lock contention under parallel `SINGLETON + shareInjector = false` execution. | Real but bounded; Liquibase's `DATABASECHANGELOGLOCK` serialises correctly. Documented in `agent-docs/troubleshooting.md`. |
| Existing branch perf test (`RackinspectModelClassCachePerformanceTest`) no longer demonstrates caching under default annotation. | One-line annotation update in that test class. Captured in `tasks.md`. |
| Conceptual churn: people reading the branch history see `cacheRuntime` in commits and `shareInjector` in shipped code. | The rename happens in one focused commit on this branch; pre-ship there is no historical user. |

## Migration Plan

This change is part of the same branch as `cache-byclass-test-runtime` and
`judo-test-enable-runtime-cache-flag`. None of the three has been released.
Rollout is therefore one branch update:

1. Apply the rename + default flip in `JudoTest.java`.
2. Apply the routing simplification in `JudoTestExtensionRouting.java`.
3. Update the four test classes that exercise the cached fast path to set
   `shareInjector = true`.
4. Add the two new tests (`DefaultIsFreshRuntimePerMethodTest`,
   `SingletonShareInjectorFlagTest`).
5. Update the six documentation files (top-level + `agent-docs/`).
6. Update the two prior OpenSpec changes' specs (or document them as
   superseded by this one — see `tasks.md` for the chosen approach).
7. Run `mvn clean install` and `mvn test -Dgroups=slow` to verify both
   correctness and perf invariants.
8. `openspec validate share-injector-opt-in`.

Rollback: revert the change commit. No schema changes, no released API
removals.

## Open Questions

1. **Do we want to require `shareInjector = true` to also explicitly state
   `dataSourceMode`?** Currently `BY_METHOD + shareInjector = true` is a
   no-op (flag ignored). We could throw on this combination to force
   the author to be deliberate. Recommendation: don't — accepting
   meaningless combinations gracefully is friendlier than throwing.

2. **Should `BY_METHOD` reject `shareInjector = true` explicitly?**
   See Q1. Recommendation: tolerate (no-op).

3. **Should the BY_CLASS perf-regression test be auto-tagged with
   `shareInjector = true` via a meta-annotation?** Considered, rejected:
   too much magic, and the explicit annotation is documentation in itself.

4. **Should the original two OpenSpec changes be archived as-is, or
   should this change *modify their delta specs* before they archive?**
   Recommendation: archive the originals as historical record; this
   change's delta is the authoritative one for the final main spec.

5. **Future: is there an even cleaner design where `dataSourceMode` is
   removed in favour of `(datasourceScope, injectorScope)` two-axis
   selection?** Out of scope here, but worth a thinking session later.
