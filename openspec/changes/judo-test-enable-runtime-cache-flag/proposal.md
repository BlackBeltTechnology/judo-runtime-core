## Why

The recently-shipped `cache-byclass-test-runtime` change tightly coupled
runtime caching (Guice `Injector`, `QueryFactory`, Liquibase executor,
`PlatformTransactionManager`) to the `dataSourceMode` parameter of
`@JudoTest`: choosing `BY_CLASS` or `SINGLETON` now also enables the runtime
cache, with the only opt-out being `BY_METHOD` (which gives up datasource
sharing too).

This is a problem for tests that:

- want a **shared datasource** (for performance) but a **fresh injector** per
  method (e.g. tests that mutate Guice singleton state, mutate schema, or
  rely on subclass overrides of `JudoRuntimeFixture#init(\u2026)`);
- have **stateful interceptor instances** the user does not want to reset
  manually in `@BeforeEach`;
- run **schema-mutating** code paths but still want `BY_CLASS` HikariCP pool
  reuse across methods.

Today the only way to get any of the above is to drop to `BY_METHOD` \u2014 which
re-creates the entire datasource, runs Liquibase from scratch, and pays the
full ~28 s cold cost on every method. We need a finer-grained knob.

## What Changes

- Add a new optional element `cacheRuntime` of type `boolean` to the
  `@JudoTest` annotation, defaulting to `true` (backwards-compatible:
  current behavior is preserved).
- When `cacheRuntime = false` AND `dataSourceMode \u2208 { BY_CLASS, SINGLETON }`,
  `JudoTestExtension` SHALL skip the runtime cache and rebuild
  `QueryFactory`, the database `Module`, the Liquibase executor, the Guice
  `Injector`, and the `PlatformTransactionManager` for every test method,
  while still reusing the cached `JudoModelLoader` and the shared
  datasource for that scope.
- When `cacheRuntime = false` AND `dataSourceMode = BY_METHOD`, the flag is
  a no-op (already no caching).
- When `cacheRuntime = true` (the default), behavior is unchanged from the
  `cache-byclass-test-runtime` change.
- Documentation (`agent-docs/TEST-CONFIGURATION.md`,
  `agent-docs/api-reference.md`, `README.md`,
  `agent-docs/troubleshooting.md`) SHALL describe the new flag, its default,
  and the four mode/cache combinations.
- Functional regression tests SHALL cover the new flag for both values
  across `BY_CLASS`, `SINGLETON`, and `BY_METHOD` modes.

No existing public API element is renamed, removed, or changes its default.
The change is purely **additive**; existing `@JudoTest`-annotated tests
continue to compile and run with identical behavior.

## Capabilities

### New Capabilities

(none \u2014 this is an additive enhancement of an existing capability)

### Modified Capabilities

- `guice-testkit`: adds requirements describing the new
  `@JudoTest#cacheRuntime` element, its semantics for each `dataSourceMode`
  value, and its default.

## Impact

- **Code touched**:
  - `judo-runtime-core-guice-testkit/src/main/java/.../fixture/JudoTest.java`
    (add annotation element)
  - `judo-runtime-core-guice-testkit/src/main/java/.../fixture/JudoTestExtension.java`
    (consult the flag in `beforeEach`; route to cold path when `false`)
  - Documentation files listed above
  - New functional tests in `src/test/.../fixture/`

- **Public API**: ONE new optional annotation element with a default
  matching today's behavior. No changes to enums, no removals, no renames.
  Source-, binary-, and behavior-compatible.

- **Dependencies**: none added.

- **Build / CI**: no new modules, no new tagged tests; default `mvn test`
  picks up the new functional tests automatically.

- **Risk**: minimal. The `cacheRuntime = true` path is unchanged from today.
  The `cacheRuntime = false` path falls back to the cold path that already
  exists for `BY_METHOD` and is exercised by the existing test suite.
  Cache-key correctness, idempotent close, and SINGLETON shutdown
  invariants are unaffected (the cache is bypassed entirely when the flag
  is `false`, not partially populated).
