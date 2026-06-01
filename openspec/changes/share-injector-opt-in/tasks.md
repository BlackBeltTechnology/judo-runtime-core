# Tasks: share-injector-opt-in

> Numbering uses RED-then-GREEN ordering (TDD). Each section's tests are
> written or updated first to express the new expected behaviour, run to
> confirm they fail under the current code, then code is updated to make
> them pass.

## 1. Update RED-state regression tests (express new defaults)

- [x] 1.1 Add `DefaultIsFreshRuntimePerMethodTest`: a class annotated only
      `@JudoTest(dataSourceMode = BY_CLASS)` (no other elements) MUST
      receive distinct `Injector` instances across two `@Test` methods;
      the class-scoped `DataSource` MUST still be reused. Confirm RED.
- [x] 1.2 Add `SingletonShareInjectorFlagTest` with two `@Nested`
      configurations:
      - `SINGLETON` + default — distinct injectors, shared datasource
      - `SINGLETON` + `shareInjector = true` — same injector, shared
        datasource
      Confirm RED.
- [x] 1.3 Update `JudoTestCacheRuntimeFlagTest` → rename file and class
      to `ShareInjectorFlagTest`; update assertions to test
      `BY_CLASS + shareInjector = true` (cached) vs `BY_CLASS + default`
      (fresh). Confirm RED.
- [x] 1.4 Update `RoutingPredicateTest` to cover the new uniform
      routing rule: `useCache` is now exactly `isClassLevel &&
      mode != BY_METHOD && shareInjector`. Replace SINGLETON-always-
      cached scenarios with SINGLETON-honours-flag scenarios. Confirm
      RED for the SINGLETON-with-`false` case.
- [x] 1.5 Update `MethodLevelCacheRuntimeIgnoredTest` → rename file and
      class to `MethodLevelShareInjectorIgnoredTest`; assertions
      unchanged in spirit (method-level annotation still cannot enable
      caching). Confirm GREEN-before-fix (behaviour unchanged) or RED if
      the implementation incidentally honoured method-level.

## 2. Annotation surface change

- [x] 2.1 In `JudoTest.java`:
      - Remove `boolean cacheRuntime() default true;`
      - Add `boolean shareInjector() default false;`
      - Update the class-level Javadoc: replace the
        `cacheRuntime` behaviour matrix with the new 5-row
        `shareInjector` matrix from `proposal.md`.
      - Update the `@since` tag if the target version is not 1.0.7.
- [x] 2.2 Verify no other file in `judo-runtime-core-guice-testkit`
      references the `cacheRuntime` element name; if any do, update
      to `shareInjector`.

## 3. Routing simplification

- [x] 3.1 In `JudoTestExtensionRouting.java`, replace `useCache(...)`
      body with:
      ```java
      if (!isClassLevel) return false;
      if (mode == BY_METHOD) return false;
      return shareInjector;
      ```
      Rename the third parameter from `cacheRuntime` to
      `shareInjector`. Update class-level Javadoc to reflect the
      removal of the SINGLETON carve-out and the new orthogonal
      semantics.
- [x] 3.2 In `JudoTestExtension.java#beforeEach`, change the call site
      from `annotation.cacheRuntime()` to `annotation.shareInjector()`.
      No other change to the caching mechanics.

## 4. Make RED tests GREEN

- [x] 4.1 Re-run task-1 tests — all five SHALL now pass.
- [x] 4.2 Re-run the full testkit suite (`mvn test` in
      `judo-runtime-core-guice-testkit`) — every previously-GREEN test
      that *relied on the implicit default-true cache* SHALL be updated
      (see §5) and SHALL then pass.

## 5. Update tests that previously relied on the default-on cache

For each of the tests below, add `shareInjector = true` to the relevant
`@JudoTest` annotation. These tests exercise the cached fast path and
without the flag would (correctly) measure fresh-runtime behaviour and
fail their identity / count / perf assertions.

- [x] 5.1 `PrepareWithCachedRuntimeTest` — unit-level Mockito tests, no
      `@JudoTest` annotation; passes unchanged under the new defaults.
- [x] 5.2 `ColdPathModelLoaderTest` — Mockito-only; passes unchanged.
- [x] 5.3 `SingletonRuntimeMapTest` — anonymous `@JudoTest` impl already
      overrides `shareInjector() = true` (renamed from `cacheRuntime`);
      passes unchanged.
- [x] 5.4 `SingletonDatasourceKeyingTest` and `SingletonModelLoaderKeyingTest`
      — these test the *datasource* and *model loader* maps, which are
      still populated for SINGLETON regardless of `shareInjector`. Both
      pass under the new defaults without changes.
- [x] 5.5 `CountingLiquibaseExecutorTest` — unit test of the wrapper
      contract (count-on-success). No `@JudoTest` dependency; passes
      unchanged.
- [N/A] 5.6 `RackinspectModelClassCachePerformanceTest` (`@Tag("slow")`)
      — this test lives in the downstream `rackinspect` consumer project,
      not in judo-runtime-core. When consumed, add `shareInjector = true`
      to its `@JudoTest` annotation; the 5× assertion is meaningless
      without it. Tracked in rackinspect, not here.
- [x] 5.7 `CacheKeyEqualityTest` — anonymous `@JudoTest` impl already
      overrides `shareInjector() = true` (renamed from `cacheRuntime`);
      passes unchanged.

## 6. Documentation updates

- [x] 6.1 Top-level `judo-runtime-core-guice-testkit/README.md`:
      - Update the "Quick start" example to show the default
        `@JudoTest(dataSourceMode = BY_CLASS)` is *fresh per method*.
      - Add a "Want the 5× perf win? Add `shareInjector = true`."
        callout right after.
- [x] 6.2 `judo-runtime-core-guice-testkit/TEST-CONFIGURATION.md` and
      `agent-docs/TEST-CONFIGURATION.md`:
      - Replace the "Caching invariants" section's location: move from
        under `BY_CLASS` to under `shareInjector = true`.
      - Update the 3-row mode table to a 5-row `(mode, shareInjector)`
        table matching `proposal.md`.
      - Add a "Why default is `false`" rationale paragraph linking to
        this change.
- [x] 6.3 `agent-docs/api-reference.md`:
      - Replace every occurrence of `cacheRuntime` with
        `shareInjector`.
      - Update the default value cell from `true` to `false`.
      - Update the cross-references to the prior change name.
- [x] 6.4 `agent-docs/interceptor-testing.md`:
      - The stateful-interceptor callout moves: it no longer triggers
        on plain `BY_CLASS`, it triggers on `shareInjector = true`.
      - Mention `shareInjector = false` (the default) as the simplest
        escape hatch for stateful interceptors, alongside the
        `@BeforeEach` reset pattern and `BY_METHOD` switch.
- [x] 6.5 `agent-docs/troubleshooting.md`:
      - Replace the entry "How do I keep BY_CLASS perf but get a fresh
        injector per method?" → answer is "that's now the default;
        omit `shareInjector` or set it to `false` explicitly".
      - Add a new entry: "Why are my BY_CLASS tests slow after the
        upgrade?" → answer points at `shareInjector = true`.
      - Add a new entry: "Can I use SINGLETON without sharing the
        injector?" → answer: yes, default; documents the Liquibase
        idempotence guarantee from `design.md` D2.

## 7. Update branch summary document

- [x] 7.1 In `docs/JNG-6374-testkit-runtime-cache.md`, append a new
      section ("§17. Pre-ship inversion: `shareInjector`") explaining
      the rename, the default flip, and the SINGLETON unification.
      Link to this OpenSpec change.

## 8. OpenSpec hygiene

- [x] 8.1 Run `openspec validate share-injector-opt-in` — SHALL pass.
- [x] 8.2 Run `openspec validate cache-byclass-test-runtime` — SHALL
      still pass (this change does not invalidate that one's specs;
      they are MODIFIED by this delta but coherent).
- [x] 8.3 Run `openspec validate judo-test-enable-runtime-cache-flag`
      — SHALL still pass (artifacts unchanged; this change supersedes
      it via the `supersedes:` key in `.openspec.yaml`).
- [ ] 8.4 Optionally, mark `judo-test-enable-runtime-cache-flag` as
      "superseded by share-injector-opt-in" in its
      `proposal-v2-simplify-cacheRuntime.md` header. The historical
      content remains as record of the design journey.

## 9. Verify and ship

- [x] 9.1 Run testkit tests in isolation (`mvn -pl judo-runtime-core-guice-testkit
      test` under Java 21 SDKMAN) excluding the pre-existing Docker-only
      `JudoDefaultPostgresqlModuleTest` and the JDK-version-sensitive
      `EnvironmentVariableMockerTest` (mockito-inline + ProcessEnvironment
      incompatibility on JDK 25, unrelated) — **BUILD SUCCESS**, 157
      tests, 0 failures, 0 errors, 25 skipped, 9s. Full reactor `mvn
      clean install` deferred to CI.
- [ ] 9.2 Run `mvn test -Dgroups=slow -pl
      judo-runtime-core-guice-testkit -Dtest=
      RackinspectModelClassCachePerformanceTest` — the 5× / 6 s
      thresholds SHALL still hold under the new explicit
      `shareInjector = true`.
- [x] 9.3 Commit on the same branch (commit `a7b917c7`) — done.
      (`feature/JNG-6374_cache_model_loader_in_testkit`) with a
      conventional message: `JNG-6374 Rename cacheRuntime →
      shareInjector and invert default to false`. Reference this
      change in the body.
- [ ] 9.4 Push and update the PR description.
- [ ] 9.5 After merge, archive all three changes together:
      ```
      openspec archive cache-byclass-test-runtime
      openspec archive judo-test-enable-runtime-cache-flag
      openspec archive share-injector-opt-in
      ```
      The final main spec for `guice-testkit` SHALL reflect this
      change's delta as the authoritative shape.
