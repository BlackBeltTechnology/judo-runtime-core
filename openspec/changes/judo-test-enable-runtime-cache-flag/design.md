## Context

The `cache-byclass-test-runtime` change introduced a per-scope cache of
derived runtime artifacts (`QueryFactory`, database `Module`, Liquibase
executor, Guice `Injector`, `PlatformTransactionManager`) for
`@JudoTest(dataSourceMode = BY_CLASS | SINGLETON)`. The cache is currently
**unconditional** for those two modes; the only way to bypass it is to fall
back to `BY_METHOD`, which also gives up datasource and model-loader
sharing.

Empirically the cache pays off (~20\u00d7 speed-up on a 20-method class), but
some test classes legitimately need a fresh injector per method while
keeping the rest of the BY_CLASS infrastructure:

- tests with stateful interceptors that the user does not want to reset in
  `@BeforeEach`;
- tests that mutate the schema (a dropped table breaks the cached injector
  but Liquibase is not re-run on the next method);
- tests with a custom `JudoRuntimeFixture` subclass overriding `init(\u2026)`,
  whose override is bypassed on the cached fast path.

Today these tests must use `BY_METHOD` and pay the full ~28 s cold cost on
every method.

## Goals / Non-Goals

**Goals:**
- Add a single, optional, backwards-compatible `boolean cacheRuntime`
  element to `@JudoTest`, defaulting to `true`.
- When `cacheRuntime = false` AND `dataSourceMode \u2208 { BY_CLASS, SINGLETON }`,
  rebuild `QueryFactory`, the database `Module`, the Liquibase executor,
  the Guice `Injector`, and the `PlatformTransactionManager` per method,
  while keeping the cached `JudoModelLoader` and the shared datasource for
  the scope.
- Preserve the public API surface: no rename, no removal, no enum changes.
- Preserve all existing behavior when `cacheRuntime` is unspecified.

**Non-Goals:**
- Decoupling `JudoModelLoader` caching from `dataSourceMode` (model load
  is read-only and continues to follow the datasource scope).
- Decoupling Liquibase re-execution from injector rebuild (when
  `cacheRuntime = false`, Liquibase re-runs because the cold path always
  runs Liquibase via `RdbmsInit#execute` \u2014 this is acceptable and
  documented).
- Introducing a separate `runtimeMode` enum (Option B in the proposal
  discussion). Rejected for now; the boolean is sufficient to unlock the
  three concrete pain points listed in Context.
- Renaming `dataSourceMode` to `testIsolationMode` (Option C). Out of scope
  \u2014 deferred until a stronger justification appears.

## Decisions

### D1. Single boolean, default `true`

Add `boolean cacheRuntime() default true;` to `@JudoTest`. Rationale:

- **Backwards-compatible**: every existing `@JudoTest`-annotated test
  compiles and runs with identical behavior.
- **Discoverable**: a single boolean is easier to find in IDE
  autocompletion than a new enum.
- **Sufficient**: the three pain points (stateful interceptors, schema
  mutation, custom `init` override) all need the same coarse-grained
  behavior \u2014 "rebuild the injector and everything bound to it per method"
  \u2014 which a boolean expresses cleanly.

Alternatives considered:
- `runtimeMode = BY_METHOD | BY_CLASS | SINGLETON` (separate enum). More
  orthogonal but introduces invalid combinations to validate (e.g.
  `dataSourceMode = BY_METHOD, runtimeMode = BY_CLASS` is impossible
  because the injector binds the per-method DataSource). Rejected as
  over-engineered for the current need.
- Three separate booleans (`cacheInjector`, `cacheLiquibase`,
  `cacheInterceptors`). Rejected: high API surface, no concrete user need
  for the fine grain today.

### D2. Routing in `JudoTestExtension.beforeEach`

Today's `beforeEach` chooses between cold path and cached path with:

```java
boolean useCache = isClassLevel
        && (mode == BY_CLASS || mode == SINGLETON);
```

Change to:

```java
boolean useCache = isClassLevel
        && (mode == BY_CLASS || mode == SINGLETON)
        && annotation.cacheRuntime();
```

When `useCache` is `false`, control flows into the existing cold path that
already exists for `BY_METHOD` and method-level annotations. That path
calls `prepare(\u2026) \u2192 init(\u2026)` and rebuilds the injector. The cached
`JudoModelLoader` (set by `beforeAll` for BY_CLASS / SINGLETON) is still
consulted by the cold path's existing `cachedModelLoader != null` check,
so the model is reused. The shared `DataSource` (also set by `beforeAll`)
is reused trivially because it lives in the same class-scoped store.

Rationale: re-uses an existing well-tested code path; the new flag is a
single conjunct in one boolean expression. No new branches, no new
helpers.

### D3. No change to the cache lookup or shutdown logic

When `cacheRuntime = false`, `getOrBuildCachedRuntime` is never called and
the static `singletonRuntimes` map is never written. Cache key correctness,
idempotent close, and the SINGLETON shutdown hook are therefore
unaffected. There is no possibility of a partially-populated cache: the
flag is consulted at the top of the routing decision, before any cache
lookup.

### D4. Annotation visible at class level only

`@JudoTest#cacheRuntime` is honoured for class-level annotations. For
method-level `@JudoTest`, the existing rule applies: method-level
annotation always behaves as `BY_METHOD`, where caching is irrelevant.
The flag value is ignored on method-level annotations \u2014 no error, no
warning. Documented in the spec.

### D5. Documentation strategy

Update the existing four caching-related sections introduced by the
previous change to describe the new flag:

- `agent-docs/TEST-CONFIGURATION.md` \u00a7 *Caching invariants*: add the
  five-row behavior matrix and the new escape hatch.
- `agent-docs/api-reference.md` \u00a7 *@JudoTest Attributes*: add the new
  element.
- `agent-docs/troubleshooting.md`: add a new entry "How do I keep BY_CLASS
  perf but get a fresh injector per method?" \u2192 `cacheRuntime = false`.
- `agent-docs/interceptor-testing.md` \u00a7 *Stateful interceptors callout*:
  add `cacheRuntime = false` as an alternative to `BY_METHOD` and the
  `@BeforeEach` reset pattern.
- Top-level `README.md` and `TEST-CONFIGURATION.md`: same updates as the
  agent-docs counterparts (these files are kept in sync).

## Risks / Trade-offs

- **Liquibase re-runs every method when `cacheRuntime = false`** \u2192
  Documented limitation. Tests that need to skip Liquibase re-runs but get
  a fresh injector per method cannot be expressed with this boolean alone;
  they must either reset state in `@BeforeEach` under `cacheRuntime = true`,
  or accept the Liquibase cost. A future finer-grained flag could address
  this if a real user need emerges.
- **Counter-intuitive name**: a user reading `cacheRuntime = false` might
  expect the model loader and datasource to also be per-method. They are
  not (those are still controlled by `dataSourceMode`). \u2192 Mitigation:
  documentation table explicitly enumerates the five mode/flag
  combinations.
- **Easy to mis-use as performance "fix"**: a user hitting a flaky test
  could disable the cache to make it pass without understanding why. \u2192
  Mitigation: troubleshooting docs lead with `@BeforeEach` reset and
  `BY_METHOD` before mentioning `cacheRuntime = false`.
- **Increased combinatorial test surface**: 5 reachable mode/flag
  combinations instead of 3. \u2192 Mitigation: functional tests at the unit
  level cover the routing decision in isolation; integration coverage
  remains gated on the rackinspect-or-equivalent model availability
  (unchanged by this proposal).

## Migration Plan

Pure additive change. Rollout:

1. Add the annotation element with default `true`. Re-run full reactor;
   nothing should change (default preserves current behavior).
2. Wire the flag into `JudoTestExtension.beforeEach` (one-line change to
   the `useCache` expression).
3. Add functional tests for both flag values across the three datasource
   modes.
4. Update documentation.
5. Validate with `openspec validate judo-test-enable-runtime-cache-flag`.

Rollback: revert PR. No schema changes, no API removals.

## Open Questions

- Should we emit a `log.warn(\u2026)` when `cacheRuntime` is set on a
  method-level `@JudoTest` (where it has no effect)? Default: no \u2014 the
  ignored case is documented and a runtime warning would just be noise.
  Revisit if users report confusion.
- Is there appetite for a follow-up `runtimeMode` enum that also unlocks
  `(SINGLETON datasource, BY_CLASS injector)`? Out of scope; track as a
  separate proposal if a user reports the need.
