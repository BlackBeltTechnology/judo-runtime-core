## 1. Functional regression tests (TDD)

- [x] 1.1 Add `JudoTestCacheRuntimeFlagTest` covering the routing decision in `JudoTestExtension.beforeEach` for the new flag, asserting:
      - default value of `cacheRuntime` on the annotation is `true`
      - `(BY_CLASS, true)` and `(BY_CLASS, default)` route to the cached path
      - `(BY_CLASS, false)` routes to the cold path
      - `(SINGLETON, false)` routes to the cold path AND does NOT populate `JudoTestExtension.singletonRuntimesForTesting()`
      - `(BY_METHOD, true)` and `(BY_METHOD, false)` are observably identical (cold path in both cases)
      - confirm tests fail today (annotation element does not exist yet)
- [x] 1.2 Add `MethodLevelCacheRuntimeIgnoredTest` asserting that `@JudoTest(cacheRuntime = true)` placed on a METHOD produces the same behavior as omitting the flag (cold path) — confirm tests fail today

## 2. Annotation surface change

- [x] 2.1 Add `boolean cacheRuntime() default true;` element to `JudoTest.java` with full Javadoc describing:
      - default behavior (caching enabled for BY_CLASS / SINGLETON)
      - effect of `false` (rebuild Injector / QueryFactory / databaseModule / Liquibase executor / TxManager per method, while keeping shared DataSource and JudoModelLoader)
      - explicit note that the flag is ignored for BY_METHOD and method-level annotations
      - cross-reference to `agent-docs/TEST-CONFIGURATION.md § Caching invariants`

## 3. Routing change in JudoTestExtension

- [x] 3.1 Update the `useCache` expression in `JudoTestExtension.beforeEach` to also conjoin `annotation.cacheRuntime()` (single-line edit)
- [x] 3.2 Verify the cold path still consults the cached `JudoModelLoader` (no regression to existing model-load amortisation)
- [x] 3.3 Verify the cold path still uses the per-class / JVM-wide `DataSource` (no regression to existing datasource sharing)

## 4. Make the failing tests GREEN

- [x] 4.1 Re-run tests 1.1 and 1.2 — SHALL now pass
- [x] 4.2 Re-run all existing testkit tests (`mvn test` in `judo-runtime-core-guice-testkit`) — SHALL still pass with no regressions

## 5. Documentation

- [x] 5.1 Update `judo-runtime-core-guice-testkit/agent-docs/TEST-CONFIGURATION.md § Caching invariants` with a five-row mode/flag behavior matrix and an "escape hatches" subsection mentioning `cacheRuntime = false` alongside `BY_METHOD`
- [x] 5.2 Update `judo-runtime-core-guice-testkit/agent-docs/api-reference.md § @JudoTest Attributes` with the new `cacheRuntime` element
- [x] 5.3 Update `judo-runtime-core-guice-testkit/agent-docs/troubleshooting.md` with a new entry "How do I keep BY_CLASS perf but get a fresh injector per method?" -> `cacheRuntime = false`
- [x] 5.4 Update `judo-runtime-core-guice-testkit/agent-docs/interceptor-testing.md § Stateful interceptors callout` to list `cacheRuntime = false` as an alternative to `BY_METHOD` and the `@BeforeEach` reset pattern
- [x] 5.5 Sync top-level `judo-runtime-core-guice-testkit/README.md` and `TEST-CONFIGURATION.md` with the agent-docs updates
- [x] 5.6 Add a one-line entry to `judo-runtime-core-guice-testkit/agent-docs/README.md § Recent Changes` referencing JNG-6374

## 6. DRY refactoring (retrospective cleanup)

- [x] 6.1 Extract `resolveDialect(String)` in `JudoRuntimeFixture` — eliminated 3x duplicated dialect instantiation
- [x] 6.2 Refactor `loadModel()` to delegate to static `loadModelFromFilesystem/Classpath/Auto` helpers — eliminated duplicate model-loading logic between `prepare()` and `loadModel()`
- [x] 6.3 Extract `resolveDialectName(JudoTest)` in `JudoTestExtension` — eliminated 2x duplicated env-var/annotation/default resolution
- [x] 6.4 Reset `singletonModelLoader = null` in `closeAllSingletonRuntimes()` — prevent stale model leaking across test suites
- [x] 6.5 Fix `JNG-XXXX` placeholders -> `JNG-6374` in `agent-docs/README.md`
- [x] 6.6 Fix `@since` tag -> `1.0.7` in `JudoTest.java`
- [x] 6.7 Fix `api-reference.md` method name (`prepare(JudoModelLoader,...)` -> `prepareWithModel(...)`) and add `resolveDialect`/`loadModel` to static helpers table
- [x] 6.8 Re-run `mvn test` — 146 tests, 0 failures, 0 errors

## 7. Verify and ship

- [x] 7.1 Run `mvn test` — 146 tests passed, 0 failures
- [ ] 7.2 Open PR with the proposal, design, specs, and tasks attached; request review
- [ ] 7.3 After merge, archive the change with `openspec archive judo-test-enable-runtime-cache-flag`
