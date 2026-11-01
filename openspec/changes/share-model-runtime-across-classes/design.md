## Context

`JudoTestExtension` has three JVM-wide maps (`singletonDatasources`, `singletonModelLoaders`,
`singletonRuntimes`) used only by `SINGLETON`. `BY_CLASS` loads a fresh `JudoModelLoader` per class
(`initializeModelLoader`) and `buildCachedRuntime` → `JudoRuntimeFixture#prepareWithModel` →
`initQueryFactory()` builds a fresh `QueryFactory` per class. `JudoRuntimeFixture#init` already
passes `queryFactory` and `coercer` into `JudoDefaultModule`, so the injector uses whatever
`QueryFactory` the fixture holds.

## Goals / Non-Goals

**Goals:** build model loader + `QueryFactory` once per JVM for `BY_CLASS` classes with the same
`(modelName, dialect, modelSource)`; keep DB / schema / injector per class; zero behaviour change
unless opted in.

**Non-Goals:** sharing the injector or datasource across classes (that is `SINGLETON`); changing
`BY_METHOD` / method-level semantics; changing the public `@JudoTest` annotation.

## Decisions

### D1 — Opt-in via system property, not an annotation attribute
`judo.testkit.shareModel=true` (read with `Boolean.getBoolean`, like `judo.testkit.configureLogging`).
- *Why:* it is a suite-level performance choice; consumers set it once in surefire
  `systemPropertyVariables` instead of editing hundreds of annotations. The default keeps the
  contract pinned by `DefaultIsFreshRuntimePerMethodTest` (fresh runtime by default).
- *Alternative:* `@JudoTest(shareModel = true)` — rejected: per-class opt-in buys nothing (the cache
  only pays off across classes) and costs a mass annotation edit.

### D2 — Split the model-derived part into `ModelRuntime`
New package-private immutable holder `ModelRuntime(modelLoader, coercer, queryFactory)` with
`static ModelRuntime build(JudoModelLoader)` — the body of today's `initQueryFactory()`.
`JudoRuntimeFixture#initQueryFactory()` becomes `applyModelRuntime(ModelRuntime.build(modelHolder))`
(behaviour identical). New `prepareWithModelRuntime(ModelRuntime, DataSource, String)` installs a
pre-built one and runs only `initModules` (fresh Liquibase schema on the class's DataSource).
- *Why:* one concept per cache — `ModelRuntime` = read-only, model-derived, JVM-shareable;
  `CachedRuntime` = stateful, scope-bound. `CachedRuntime` is unchanged (still references the
  `QueryFactory`/coercer it was built with).

### D3 — JVM-wide map keyed by `SingletonModelKey`
`static ConcurrentHashMap<SingletonModelKey, ModelRuntime> sharedModelRuntimes`. The model loader
inside it is taken from (and stored into) the existing `singletonModelLoaders` map, so a
`SINGLETON` class and a shared `BY_CLASS` class with the same key reuse one model load. Creation
uses the existing double-checked `synchronized (JudoTestExtension.class)` pattern (`loadModel`
throws checked exceptions). The map lives for the **JVM**, not the launcher session: surefire runs one launcher
session per test class in a reused fork, and the root store — hence `closeAllSingletonRuntimes()` —
closes at the end of every session. Clearing the map there made every fork build the runtime twice
(found against rackinspect: 8 builds for 4 forks). The model runtime holds no resources (only
memory), so nothing needs closing; tests reset it via `clearSharedModelRuntimesForTesting()`.
- *Observation, out of scope:* `singletonShutdownRegistered` is never reset after
  `closeAllSingletonRuntimes()`, so after the first session the hook is not re-registered and
  SINGLETON resources created later are not closed at session end. Worth a separate ticket.

### D4 — Where it is applied
- `initializeModelLoader`, `BY_CLASS` branch: when `shareModel()` → store the shared model loader
  under `MODEL_LOADER_KEY` instead of loading a new one.
- `buildCachedRuntime`: when `shareModel()` and mode is `BY_CLASS` → `prepareWithModelRuntime(...)`
  instead of `prepareWithModel(...)`.
- `BY_CLASS` + `shareInjector = false` cold path (`prepareWithModel` per method) also benefits from
  the shared model loader; it keeps building its own `QueryFactory` per method (unchanged contract).
- `BY_METHOD`, method-level `@JudoTest`, `SINGLETON`: untouched.

## Risks / Trade-offs

- [Shared EMF `ResourceSet` / `QueryFactory` internal `ConcurrentHashMap` across classes] →
  same exposure `SINGLETON` already has; `BY_CLASS` caching already requires no in-fork class
  parallelism. Documented.
- [A test mutates the loaded model] → its change leaks to later classes in the fork. Documented
  caveat: such tests must not run with the property on.
- [Memory] → one model + `QueryFactory` per key per fork instead of per class: strictly less.

## Migration Plan

Consumers add `<judo.testkit.shareModel>true</judo.testkit.shareModel>` to surefire
`systemPropertyVariables`. Rollback = remove the property.
