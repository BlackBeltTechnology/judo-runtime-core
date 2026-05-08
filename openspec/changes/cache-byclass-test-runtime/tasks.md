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

## 8. Verify and ship

- [x] 8.1 Run `mvn clean install` (excluding the pre-existing Docker-only `JudoDefaultPostgresqlModuleTest` per project convention) — full reactor SHALL succeed
- [x] 8.2 Run `openspec validate cache-byclass-test-runtime` — SHALL pass
- [ ] 8.3 Open PR with the proposal, design, specs, and tasks attached; request review
- [ ] 8.4 After merge, archive the change with `openspec archive cache-byclass-test-runtime`
