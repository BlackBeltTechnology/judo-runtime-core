## Why

`BY_CLASS` + `shareInjector = true` (the cached fast path from `cache-byclass-test-runtime`)
removed the per-*method* bootstrap cost, but every test *class* still rebuilds the whole runtime.
On a large real-world model (rackinspect, ~300 `@JudoTest` classes) a JFR profile of one class
shows where the ~28 s first-test cost goes:

| Step | Wall time |
|---|---|
| `JudoRuntimeFixture#initQueryFactory` → `QueryFactory.<init>` (`ExpressionEvaluator.init`, `createQueries` on the ForkJoin common pool) | **~21 s** |
| `JudoRuntimeFixture.loadModel` (ASM/RDBMS/measure/expression/liquibase, validated) | ~1–2.5 s |
| Liquibase changelog execution (~250 changeSets) | ~0.5–0.8 s |
| `Guice.createInjector` | ~0.1 s |

The model loader and the `QueryFactory` depend only on `(modelName, dialect, modelSource)` and are
read-only after construction, yet `BY_CLASS` rebuilds them for every class
(`JudoTestExtension#initializeModelLoader`, `BY_CLASS` branch). The only mode that shares them
JVM-wide is `SINGLETON`, which also shares the database — unacceptable for suites that need per-class
isolation.

Because the `QueryFactory` build is parallel and CPU-bound, concurrent surefire forks also contend
for cores, so the per-class cost grows during a long run (26 s → 44 s observed).

A spike in rackinspect (same 122 classes / 657 tests, `forkCount = 4`) measured
**23:58 → 8:26 wall clock, 0 failures** with the model loader + `QueryFactory` shared JVM-wide and
everything stateful kept per class.

## What Changes

- New **opt-in** system property `judo.testkit.shareModel` (default `false`).
- When enabled, `BY_CLASS` test classes take the `JudoModelLoader` **and** a `QueryFactory` /
  `ExtendableCoercer` pair from a JVM-wide cache keyed by `SingletonModelKey`
  `(modelName, dialect, modelSource)` — built once per fork.
- Still **per class** (unchanged): `DataSource`, Liquibase schema, database module, Guice `Injector`,
  `PlatformTransactionManager`, interceptor provider. Per-class data isolation is preserved.
- New package-private `ModelRuntime` value holder (model loader + coercer + `QueryFactory`);
  `JudoRuntimeFixture#initQueryFactory` builds through it, and a new
  `prepareWithModelRuntime(...)` installs a pre-built one.
- The shared model-runtime map lives for the JVM: it survives the end of a launcher session (surefire runs one per test class in a reused fork); it holds no resources to close.
- Unchanged: default behaviour (property unset), `BY_METHOD`, method-level `@JudoTest`, `SINGLETON`,
  `shareInjector = false` cold path, public `@JudoTest` API.
- Docs: new row in the `TEST-CONFIGURATION.md` mode matrix, caveat for tests that mutate the model.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `guice-testkit`: adds an opt-in JVM-wide sharing of the model-derived runtime for `BY_CLASS`.

## Impact

- Module: `judo-runtime-core-guice-testkit` only (`JudoTestExtension`, `JudoRuntimeFixture`,
  new `ModelRuntime`, tests, `README.md` / `TEST-CONFIGURATION.md` / `agent-docs/`).
- No production runtime code, no API break; consumers opt in with one surefire system property.
- Risk: tests that mutate the loaded EMF model at runtime would leak into later classes in the same
  fork — documented; they must not opt in.
