## 1. Failing regression-guard tests (RED)

- [x] 1.1 Add `ByClassRuntimeCacheTest` asserting `assertSame` on `Injector`, `QueryFactory`, `PlatformTransactionManager` across two ordered methods of a `BY_CLASS` test class — confirm tests fail today
- [x] 1.2 Add `ByMethodIsolationTest` asserting `assertNotSame` on `Injector` across two ordered methods of a `BY_METHOD` test class — confirm tests pass today (acts as future regression guard)
- [x] 1.3 Add `SingletonRuntimeCacheTest` mirroring 1.1 but with `dataSourceMode = SINGLETON` and two test classes sharing the singleton key — confirm tests fail today
- [x] 1.4 Add `ByClassLiquibaseSingleExecutionTest` using a counting Liquibase executor and asserting `executionCount == 1` after multiple methods — confirm tests fail today
- [x] 1.5 Add `CacheKeyEqualityTest` asserting that two `@JudoTest` configurations differing only in `modules`/`interceptors` produce different cache entries — confirm tests fail today
- [x] 1.6 Add `CacheCloseableResourceTest` asserting cached `Injector` and HikariCP pool close exactly once after class teardown (no double-close, no leak) — confirm tests fail today
- [x] 1.7 Add `BackwardsCompatBuildModeTest` with a `JudoRuntimeFixture` subclass overriding `init(...)`; assert it is invoked under `BY_METHOD` — confirm tests pass today (acts as future regression guard)

## 2. Cache infrastructure types (package-private)

- [x] 2.1 Introduce `CachedRuntime` immutable class bundling `(modelLoader, queryFactory, coercer, databaseModule, liquibaseExecutor, injector, transactionManager)` implementing `ExtensionContext.Store.CloseableResource` with idempotent `close()`
- [x] 2.2 Introduce `ByClassCacheKey` value-equal key over `(modelName, dialect, modelSource, testClass, modules, interceptors)`
- [x] 2.3 Introduce `CountingLiquibaseExecutor` delegating wrapper around `SimpleLiquibaseExecutor` with a package-private `int executionCount()` accessor

## 3. JudoRuntimeFixture fast path

- [x] 3.1 Add `prepareWithCachedRuntime(CachedRuntime cached, Object injectModulesTo)` that installs cached fields and runs `injector.injectMembers(...)` only when `injectModulesTo` is non-null
- [x] 3.2 Add package-private getters `queryFactory()` and `transactionManager()` for tests
- [x] 3.3 Ensure `tearDown()` does NOT close the injector or datasource on cached scopes; cleanup happens via `CachedRuntime#close()` only

## 4. JudoTestExtension caching orchestration

- [x] 4.1 Build `ByClassCacheKey` from the resolved `@JudoTest` annotation
- [x] 4.2 Implement BY_CLASS path: `store.computeIfAbsent(KEY, k -> buildAndExecuteOnce(...))` where `buildAndExecuteOnce` performs the existing prepare + init chain and bundles the result into a `CachedRuntime`; subsequent methods call `runtimeFixture.prepareWithCachedRuntime(cached, testInstance)`
- [x] 4.3 Implement SINGLETON path: a `static ConcurrentHashMap<ByClassCacheKey, CachedRuntime>` guarded with the same lock pattern as `singletonModelLoader`; register a single root-store `CloseableResource` that closes every entry exactly once at JVM shutdown
- [x] 4.4 Leave the BY_METHOD code path unchanged (must continue calling `runtimeFixture.init(...)`)
- [x] 4.5 In the cached path, install the `CountingLiquibaseExecutor` so subsequent test methods can verify exactly-once execution via the regression test 1.4

## 5. Make the failing tests GREEN

- [x] 5.1 Re-run tests 1.1, 1.3, 1.4, 1.5, 1.6 — all SHALL now pass
- [x] 5.2 Re-run tests 1.2, 1.7 — SHALL still pass (BY_METHOD untouched)
- [x] 5.3 Run the full module test suite (`mvn test` in `judo-runtime-core-guice-testkit`) and ensure no existing test regressed

## 6. Performance regression guard

- [x] 6.1 Tighten the existing `RackinspectModelClassCachePerformanceTest` final assertion to `cachedAvgNs * 5 < firstNs` AND `cachedAvgNs < TimeUnit.SECONDS.toNanos(6)`
- [ ] 6.2 Run the perf test with `-Dgroups=slow` against the project's CI hardware; record the observed numbers in the PR description
- [ ] 6.3 If actual speed-up is below 5× or cached avg exceeds 6 s, investigate and fix before merging (do NOT weaken the threshold)

## 7. Documentation

- [x] 7.1 Update `judo-runtime-core-guice-testkit/agent-docs/TEST-CONFIGURATION.md` with a "Caching invariants" section explaining: stateful interceptors, schema-mutating tests, and the `BY_METHOD` escape hatch
- [x] 7.2 Update `judo-runtime-core-guice-testkit/agent-docs/api-reference.md` with the new `prepareWithCachedRuntime` entry point and the `CachedRuntime` lifecycle
- [x] 7.3 Add a short changelog entry referencing JNG-XXXX in the project README or release notes

## 8. SINGLETON resource keying fix (CodeRabbit follow-up)

Addresses the regression flagged after initial implementation: the JVM-wide `singletonModelLoader` and `singletonDatasource` fields were unkeyed, causing first-write-wins collisions across SINGLETON classes with different `(modelName, dialect, modelSource)` or `(dialect, container)` configurations.

- [x] 8.1 Add `SingletonModelLoaderKeyingTest` exercising the JVM-wide model-loader map via the new `singletonModelLoadersForTesting()` accessor; asserts that two equal `SingletonModelKey`s map to the same loader and that distinct keys (varying `modelName`, `dialect`, or `modelSource`) map to distinct entries — confirm RED before fix
- [x] 8.2 Add `SingletonDatasourceKeyingTest` exercising the JVM-wide datasource map via the new `singletonDatasourcesForTesting()` accessor; asserts that distinct keys (varying `dialect` or `container`) map to distinct entries and that `closeAllSingletonRuntimes()` invokes `teardownDatasource()` on every entry exactly once — confirm RED before fix
- [x] 8.3 Introduce `SingletonModelKey` (modelName, dialect, modelSource) and `SingletonDatasourceKey` (dialect, container) value-equal key types
- [x] 8.4 Replace `private static volatile JudoModelLoader singletonModelLoader` with `ConcurrentHashMap<SingletonModelKey, JudoModelLoader> singletonModelLoaders`; replace `private static volatile CloseableDatasourceFixture singletonDatasource` with `ConcurrentHashMap<SingletonDatasourceKey, CloseableDatasourceFixture> singletonDatasources`
- [x] 8.5 Rewrite SINGLETON branches of `initializeModelLoader` and `initializeDatasource` to use double-checked lookups on the keyed maps
- [x] 8.6 Extract `resolveContainerName(JudoTest, String dialect)` helper and use it both for the cache key and inside `createDatasourceFixture` (DRY)
- [x] 8.7 Consolidate the JVM-shutdown root-store registration via a single `registerSingletonShutdownHook(ExtensionContext)` helper called from every SINGLETON resource creation site; one `singletonShutdownRegistered` flag replaces the previous separate per-runtime registration
- [x] 8.8 Extend `closeAllSingletonRuntimes()` to close every entry in `singletonDatasources` and clear `singletonModelLoaders` in addition to closing/clearing `singletonRuntimes`
- [x] 8.9 Make `CloseableDatasourceFixture` package-private so functional tests in the same package can construct instances directly
- [x] 8.10 Add package-private accessors `singletonModelLoadersForTesting()` and `singletonDatasourcesForTesting()`
- [x] 8.11 Re-run tests 8.1, 8.2 — SHALL now pass
- [x] 8.12 Re-run full testkit suite (`mvn test` in `judo-runtime-core-guice-testkit`) — SHALL still pass with no regressions (172/172 passing, 25 pre-existing skipped)

## 9. Additional CodeRabbit findings (second review)

- [x] 9.1 `CountingLiquibaseExecutor`: count only after a successful `super.createDatabase(...)` so a failed attempt followed by a retry does not inflate the count
- [x] 9.2 Rewrite `CountingLiquibaseExecutorTest` accordingly: assert counter stays 0 when super throws, and assert a subsequent successful call increments to exactly 1
- [x] 9.3 Drop the `final` modifier on `CountingLiquibaseExecutor` so the stub in the test can simulate super-success / super-failure without standing up a real Liquibase
- [x] 9.4 `prepareWithCachedRuntime` previously did NOT restore `interceptorProvider`; calling `JudoRuntimeFixture#getInterceptorProvider()` on the cached path threw a misleading `"Call init() first."` even though the user did not skip init — the extension chose the cached path. Carry `interceptorProvider` inside `CachedRuntime` and restore it in `prepareWithCachedRuntime`. Document that the provider becomes class-shared mutable state on the cached path.
- [x] 9.5 Preserve the existing 8-arg `CachedRuntime` constructor (delegates to the new 9-arg form with `null` interceptor provider) so existing unit tests do not need to change. Add `eightArgConstructorYieldsNullInterceptorProvider` to lock that contract.
- [x] 9.6 Add `PrepareWithCachedRuntimeTest#restoresInterceptorProviderOnCachedPath` as the explicit regression guard for the interceptor-provider restore.
- [x] 9.7 BY_CLASS cache lookup: replace `classStore.get(KEY)` + `classStore.put(KEY, ...)` with `classStore.getOrComputeIfAbsent(KEY, k -> buildCachedRuntime(...), CachedRuntime.class)` so two threads executing under `@Execution(CONCURRENT)` cannot both build a runtime and have one of them orphaned (un-closed by JUnit, leaking the HikariCP pool).
- [x] 9.8 Method-level `@JudoTest` annotations MUST NOT silently reuse the class-scoped preloaded model. Extract `JudoTestExtension.coldPathModelLoader(boolean isClassLevel, ExtensionContext.Store store)` and gate the read on `isClassLevel == true`.
- [x] 9.9 Add `ColdPathModelLoaderTest` with three Mockito-based unit tests (class-level reuses; method-level ignores; class-level + empty store returns null).
- [x] 9.10 Spec contradiction in `judo-test-enable-runtime-cache-flag/specs/guice-testkit/spec.md`: the old `"cacheRuntime = false Bypasses Runtime Cache for SINGLETON"` requirement directly contradicted the implementation in `JudoTestExtensionRouting.useCache` (which always caches for SINGLETON to avoid re-running Liquibase against a shared DB). Adopt the v2 design (`proposal-v2-simplify-cacheRuntime.md` Option A): replace the requirement with `"cacheRuntime is a No-Op for SINGLETON"`, mirroring the existing BY_METHOD rule. Document the rationale and the two recommended escape hatches (`@BeforeEach` reset or switch to BY_CLASS / BY_METHOD).
- [x] 9.11 Complete `RoutingPredicateTest`'s "exhaustive" claim: add the two missing `cacheRuntime = false` permutations at method-level for SINGLETON and BY_METHOD.
- [x] 9.12 Re-run full testkit suite — 180/180 passing (+8 vs round 1's 172), 25 pre-existing skipped, 0 failures, 0 errors.

## 10. Verify and ship

- [x] 10.1 Run `mvn clean install` (excluding the pre-existing Docker-only `JudoDefaultPostgresqlModuleTest` per project convention) — full reactor SHALL succeed
- [x] 10.2 Run `openspec validate cache-byclass-test-runtime` — SHALL pass
- [ ] 10.3 Open PR with the proposal, design, specs, and tasks attached; request review
- [ ] 10.4 After merge, archive the change with `openspec archive cache-byclass-test-runtime`
