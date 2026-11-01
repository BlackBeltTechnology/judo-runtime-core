## 1. Failing tests first (RED)

- [x] 1.1 `SharedModelRuntimeCacheTest` (pure unit, style of `SingletonModelLoaderKeyingTest`): equal `SingletonModelKey` → same `ModelRuntime`; distinct modelName / dialect / modelSource → distinct entries; the map survives `closeAllSingletonRuntimes()` (one launcher session per class under surefire) — confirm it fails (types/accessors missing)
- [x] 1.2 `ShareModelPropertyTest`: `shareModel()` is `false` when `judo.testkit.shareModel` is unset, `true` when set (restore the property after each test)
- [x] 1.3 `ApplyModelRuntimeTest`: `applyModelRuntime` (used by `prepareWithModelRuntime`) installs the given loader / coercer / `QueryFactory` on the fixture (`assertSame`) without building a new one
- [x] 1.4 ~~`SharedModelRuntimeIsolationTest`~~ — moved to 4.1: the testkit module has no real JUDO model (all its fixture tests are pure unit tests); the real-model isolation check runs in the consumer suite

## 2. Implementation (GREEN)

- [x] 2.1 Add package-private `ModelRuntime` with `static ModelRuntime build(JudoModelLoader)` (body moved from `initQueryFactory`)
- [x] 2.2 `JudoRuntimeFixture`: `initQueryFactory()` delegates to `ModelRuntime.build`; add `prepareWithModelRuntime(ModelRuntime, DataSource, String)`
- [x] 2.3 `JudoTestExtension`: `SHARE_MODEL_PROPERTY`, `shareModel()`, `sharedModelRuntimes` map + `sharedModelRuntimesForTesting()`, `sharedModelRuntime(annotation, dialect, context)` (double-checked lock, reuses `singletonModelLoaders`)
- [x] 2.4 Wire it in `initializeModelLoader` (`BY_CLASS`) and `buildCachedRuntime` (`BY_CLASS`); keep the map for the JVM (not cleared by `closeAllSingletonRuntimes()`), test reset via `clearSharedModelRuntimesForTesting()`
- [x] 2.5 Full module test suite green (`mvn test` in `judo-runtime-core-guice-testkit`)

## 3. Docs

- [x] 3.1 `TEST-CONFIGURATION.md` + `agent-docs/TEST-CONFIGURATION.md`: new matrix row (`BY_CLASS` + `shareInjector` + `shareModel`) and the model-mutation caveat
- [x] 3.2 `README.md` / `agent-docs/README.md`: one paragraph on enabling `judo.testkit.shareModel` via surefire `systemPropertyVariables`

## 4. Consumer verification

- [ ] 4.1 `mvn install` the reactor as `1.0.6-SNAPSHOT`; run rackinspect's integration suite against the snapshot with the property on; record wall clock before/after in the PR description
