# Proposal: Inverted, renamed sharing flag — `shareInjector` opt-in

Rename `@JudoTest#cacheRuntime` to `@JudoTest#shareInjector`, flip its default
from `true` to `false`, and make it **honour for every mode that has a notion
of sharing**, including `SINGLETON`. The result is an orthogonal two-axis API
that is safe-by-default and explicit about what it costs.

## Why

The current shape (shipped on this branch but not yet released) conflates two
orthogonal concerns under the `dataSourceMode` selector:

1. **Resource lifecycle** — how long does the physical datasource live?
   (per method / per class / per JVM)
2. **Behavioural sharing** — do two test methods receive the same `Injector`,
   the same `QueryFactory`, the same interceptor instances, the same Liquibase
   high-water-mark?

Today, selecting `BY_CLASS` automatically grants both. Selecting `SINGLETON`
grants both *and* ignores the only existing escape hatch (`cacheRuntime`).
This means:

- A test author who wants the datasource sharing perf win for `BY_CLASS` is
  forced into accepting class-shared mutable Guice / interceptor state, with
  no way to opt out except switching the entire mode to `BY_METHOD`.
- A test author who selects `SINGLETON` because they only care about a
  JVM-wide *database* connection silently gets a JVM-wide *injector* too,
  with no escape hatch at all.
- The flag we just added (`cacheRuntime`) reads as a double-negative
  (`cacheRuntime = false` means "don't cache" — an instruction to *not* do
  something), and is silently no-op'd on `SINGLETON` (a hidden special case).

The philosophical correction:

> **Resource lifecycle is selected by `dataSourceMode`.**
> **Behavioural sharing is selected by `shareInjector`.**
> **They are orthogonal. Both default to the safest value.**

Concretely: a test method should receive its own Guice graph and its own
interceptor instances **unless the test author has explicitly said otherwise**.
This is the same default pytest, JUnit 5 (`PER_METHOD`), and Testcontainers
take. Caching is a performance optimisation that the author opts into, not a
default that the author has to opt out of.

## What Changes

### Annotation surface

- Rename `@JudoTest#cacheRuntime` (default `true`) → `@JudoTest#shareInjector`
  (default `false`).
  - `cacheRuntime` is removed entirely. The previous-branch flag never shipped
    in a tagged release; downstream cost is zero.
- `shareInjector` is honoured for **both** `BY_CLASS` and `SINGLETON`
  (replacing the v2 carve-out that made `SINGLETON` ignore the flag — see
  `design.md` D2 for the new Liquibase analysis that makes
  `SINGLETON + shareInjector=false` correct and safe).
- `BY_METHOD` continues to ignore the flag (no shared runtime exists to
  share).
- Method-level `@JudoTest` annotations continue to ignore the flag (no class
  scope to attach the cache to).

### Default behaviour matrix (new)

| `dataSourceMode` | `shareInjector`         | DataSource             | Injector / Runtime           |
|------------------|-------------------------|------------------------|------------------------------|
| `BY_METHOD`      | *(ignored)*             | Fresh per method       | Fresh per method             |
| `BY_CLASS`       | `false` *(default)*     | Shared per class       | Fresh per method             |
| `BY_CLASS`       | `true`                  | Shared per class       | **Cached per class**         |
| `SINGLETON`      | `false` *(default)*     | Shared per JVM         | Fresh per method             |
| `SINGLETON`      | `true`                  | Shared per JVM         | **Cached per JVM (keyed)**   |

Two new "middle rungs" exist that the previous shape could not express:
`BY_CLASS + false` (class-level datasource sharing without injector sharing)
and `SINGLETON + false` (JVM-wide datasource sharing without injector
sharing). Both are the *defaults* for their modes.

### Behavioural consequences

- New tests written against `@JudoTest(dataSourceMode = BY_CLASS)` get
  per-method `Injector` reconstruction by default. The 5×–10× perf win from
  `cache-byclass-test-runtime` is now an explicit opt-in via
  `shareInjector = true`.
- Tests that *want* the cached fast path MUST add `shareInjector = true`
  to their class-level `@JudoTest` annotation.
- Stateful-interceptor and shared-mutable-state warnings move from "this is
  what `BY_CLASS` gives you" to "this is what `shareInjector = true` gives
  you". The default invariant — "two test methods do not see each other's
  state" — is restored.

### Documentation

- `agent-docs/TEST-CONFIGURATION.md`, `agent-docs/api-reference.md`,
  `agent-docs/interceptor-testing.md`, `agent-docs/troubleshooting.md`,
  top-level `README.md`, and top-level `TEST-CONFIGURATION.md` SHALL be
  updated to:
  - Describe `shareInjector` instead of `cacheRuntime`.
  - Move the "Caching invariants" warnings to the `shareInjector = true`
    description (positive-opt-in framing).
  - Add a "Why default is `false`" rationale section pointing at this
    proposal.

### Tests

- Every in-repo test that today relies on the implicit default-on cache
  (notably `RackinspectModelClassCachePerformanceTest`,
  `PrepareWithCachedRuntimeTest`, `JudoTestCacheRuntimeFlagTest`,
  `SingletonRuntimeMapTest`, `ColdPathModelLoaderTest`) MUST be updated to
  set `shareInjector = true` explicitly. Tests asserting fresh-runtime
  behaviour are unchanged and now match the default.
- A new `DefaultIsFreshRuntimePerMethodTest` SHALL assert that a
  `@JudoTest(dataSourceMode = BY_CLASS)` annotation with no other elements
  produces a distinct `Injector` per method.
- A new `SingletonShareInjectorFlagTest` SHALL assert both
  `SINGLETON + shareInjector=false` (fresh injector, shared datasource) and
  `SINGLETON + shareInjector=true` (cached injector, shared datasource)
  produce the documented identities.

## Capabilities

### Modified Capabilities

- `guice-testkit`:
  - The `@JudoTest` flag `cacheRuntime` is renamed to `shareInjector` and
    its default flipped from `true` to `false`.
  - `SINGLETON` mode now honours `shareInjector` (was: ignored).
  - The class-level runtime cache requirements from
    `cache-byclass-test-runtime` are re-conditioned: caching activates only
    when `shareInjector = true`.

### Removed Capabilities

- `cacheRuntime` annotation element from
  `judo-test-enable-runtime-cache-flag` — superseded by `shareInjector` in
  this change.
- The "cacheRuntime is a No-Op for SINGLETON" requirement is removed —
  superseded by the new uniform "shareInjector is honoured for SINGLETON"
  requirement.

## Impact

- **Code touched**: `judo-runtime-core-guice-testkit` only.
  - `fixture/JudoTest.java`: rename + default flip + Javadoc.
  - `fixture/JudoTestExtensionRouting.java`: drop the SINGLETON special
    case from `useCache(...)`; honour the flag uniformly.
  - `fixture/JudoTestExtension.java`: read `shareInjector()` instead of
    `cacheRuntime()`; no other change to caching mechanics.
  - The 7 tests listed above are updated; one new test added.
  - Six documentation files updated (top-level + `agent-docs/`).
- **APIs**: no public-API behaviour change for any released version
  (`cacheRuntime` never shipped in a tagged release). Internal-only
  rename.
- **Dependencies**: none added or removed.
- **Build / CI**: `mvn clean install` continues to pass. The
  `@Tag("slow")` perf regression test (`RackinspectModelClassCache-
  PerformanceTest`) now requires `shareInjector = true` in its
  `@JudoTest` annotation to demonstrate the 5× threshold; without it the
  test would (correctly) measure fresh-runtime behaviour and fail.
- **Risk**: see `design.md` Risks & Trade-offs. The principal risk is
  that downstream users adopting this branch get *slower* default tests
  than the previous default-true model promised. Mitigated by clear
  documentation and the fact that `shareInjector = true` is one keyword
  per class.

No public API is removed (because none was released). The change is
internal-design refinement before this branch ships to `develop`.
