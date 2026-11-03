# Tasks

Module tasks of `judo-runtime-core` for the master change `persisted-query-model` (judo-ng superproject,
`openspec/changes/persisted-query-model`). Rank 8: entry point, restore, shared decision.

## 1. judo-runtime-core (rank 8) -- entry point, restore, shared decision

- [ ] 1.1 Reduce `judo-runtime-core-query` to `QueryFactory` and `CustomJoinDefinition`, depending on
      `judo-tatami-asm2query`. The build path delegates to `Asm2Query` over the **caller-supplied** expression
      `ResourceSet` (unchanged semantics), copying custom joins into its value type. The `coercer` builder argument
      stays as `@Deprecated` and ignored. Verify: runtime-core compiles.
- [ ] 1.2 Add the restore path to `QueryFactory`: it wraps a `QueryIndex` from the loaded `QueryModel` (resolved
      against the runtime's ASM and expression models), with
      `AsmModelAdapter`, `FeatureFactory` and `JoinFactory` for dynamic filters and a mutable empty
      `getCustomJoinDefinitions()`; no collector. Verify: unit tests for built vs restored equality of every public
      method.
- [ ] 1.3 Implement `QueryModelLoadOrBuild` with the `PersistedQueryModel` locator: the D5 table (ASM, measure and
      expression digests), the three modes, strict `load` including custom joins, any `Exception` or `Error` →
      fallback (WARN with cause), `build` = the container's existing build, and an INFO restore line with its
      duration. Verify: a unit test for every row × mode.
- [ ] 1.4 Remove `judo-runtime-core-expression` from the reactor, the `judo-runtime-core-dependencies` BOM and the
      POMs (dao-rdbms, dao-rdbms-hsqldb, dao-rdbms-postgresql, query), adding `judo-tatami-asm2query` where needed
      (skill package: master task 2.10). Update the callers of moved types (`SelectStatementExecutor`, `FunctionMapper`, `AttributeSelectorTranslator`).
      Verify: runtime-core builds via `./project.py`.
- [ ] 1.5 Guice: `JudoModelLoader` (dir, classpath, URL) digests ASM, measure and expression model while loading
      them, and builds the locator without reading the query file. `QueryFactoryProvider` uses `QueryModelLoadOrBuild`. Add the
      `queryModelMode` option to `JudoDefaultModuleConfiguration` and the `JudoDefaultModule` builder copy. Bind no
      second `ExpressionModel`. Verify: a test matrix of dir / classpath / URL × present / absent / corrupt / stale ×
      mode.
- [ ] 1.6 Spring: the `JudoModelLoader` variant does the same; `JudoModelConfiguration` exposes an optional
      `PersistedQueryModel` bean; the `QueryFactory` bean uses `QueryModelLoadOrBuild` with
      `judo.queryModel.mode`. Expose no second `ExpressionModel` bean. Verify: Spring context tests for present /
      absent / corrupt / stale × mode, plus an app with its own model beans (builds).
- [ ] 1.7 `judo-runtime-core-guice-testkit`: the fixture obtains its `QueryFactory` through
      `QueryModelLoadOrBuild` (mode configurable). Verify: the testkit tests pass in `build` and `load`.
- [ ] 1.8 Documentation:
      - runtime-core specs `query`, `expression` (removed), `guice`, `spring` and `guice-testkit`;
      - the user-facing mode docs, including the generator-alignment requirement and `load` brittleness;
      - `AGENTS.md` rows for the changed and removed files;
      - at archive, hand-edit the Architecture prose of the store's `query` and `expression` specs, which still
        describe the moved classes.

      Verify: `openspec validate` in the runtime-core store.

## 2. Folded tests (test-plan.md)

- [ ] 2.1 Add unit test: restored QueryFactory equals built (all public methods) — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: Northwind artifacts / `QueryFactory` restore vs build: every public method on every mapped type and feature / all equal; `getCustomJoinDefinitions()` is mutable and empty on both (test-plan #TP-19)
- [ ] 2.2 Add unit test: restored ASM references are live objects — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: Northwind artifacts + live `AsmModel` / restore / every non-containment ASM target is `==` an object of the runtime's ASM resource (non-identical count = 0) (test-plan #TP-20)
- [ ] 2.3 Add unit test: Guice restore from dir / classpath jar / URL — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: valid Northwind artifacts in a dir, inside a jar on the classpath, at a file URL / `JudoModelLoader` + `QueryFactoryProvider`, mode `auto` / INFO line "query model restored … ms" for each; 0 unresolved; the JQL extractor is not invoked for the query model (spy count 0) (test-plan #TP-23)
- [ ] 2.4 Add unit test: Spring restore, single ExpressionModel bean — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: Spring context with valid artifacts on the classpath / context start / the `QueryFactory` bean is restored (INFO); exactly one `ExpressionModel` bean, same instance as before the change (test-plan #TP-24)
- [ ] 2.5 Add unit test: Guice binds no second ExpressionModel — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: Guice injector with artifacts present / `getInstance(ExpressionModel.class)` / returns the loader's `-expression.model` instance; there is no binding for the query-side model (test-plan #TP-25)
- [ ] 2.6 Add unit test: fallback: artifact absent — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: no query file / `QueryModelLoadOrBuild`, `auto` / build path taken; no WARN; factory serves `getQuery` (test-plan #TP-29)
- [ ] 2.7 Add unit test: fallback: ASM digest mismatch — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: one JQL text changed in `-asm.model` after save / boot `auto` / WARN "asmDigest mismatch"; build path; boot OK (test-plan #TP-30)
- [ ] 2.8 Add unit test: fallback: measure digest mismatch — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: one byte appended to `-measure.model` / boot `auto` / WARN "measureDigest mismatch"; build (test-plan #TP-31)
- [ ] 2.9 Add unit test: fallback: expression digest mismatch — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: `-expression.model` replaced by the one from another run of the same model / boot `auto` / WARN "expressionDigest mismatch"; build (test-plan #TP-32)
- [ ] 2.10 Add unit test: fallback: generator mismatch — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: artifact stamped `generator=1.1.6-SNAPSHOT@20261101…` vs runtime `…@20261102…` / boot `auto` / WARN names both identities; build (test-plan #TP-33)
- [ ] 2.11 Add unit test: fallback: modelName / formatVersion mismatch — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: (a) `modelName` differs; (b) `formatVersion` = supported + 1 / boot `auto` / WARN with both values; build (test-plan #TP-34)
- [ ] 2.12 Add unit test: fallback: corrupt XMI — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: `-query.model` truncated at 50% / boot `auto` / WARN with the parse cause; build; boot OK (test-plan #TP-35)
- [ ] 2.13 Add unit test: fallback: unresolved proxies — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: artifacts built against an ASM, runtime ASM with one mapped entity removed / boot `auto` / WARN "unresolved proxies: N" (N ≥ 1); build (test-plan #TP-38)
- [ ] 2.14 Add unit test: fallback: I/O error mid-stream — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: stream supplier throws `IOException` after 1 KB / boot `auto` / WARN with cause; build; boot OK (test-plan #TP-39)
- [ ] 2.15 Add unit test: fallback: Error during restore — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: restore throws `NoClassDefFoundError` (injected) / boot `auto` / WARN; build; boot OK (test-plan #TP-40)
- [ ] 2.16 Add unit test: fallback: custom joins — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: one `CustomJoinDefinition` configured / boot `auto` / INFO "saved query model not used with custom joins"; build (test-plan #TP-41)
- [ ] 2.17 Add unit test: build mode never opens the artifact — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: corrupt artifact present / boot `build` / boots; the locator's stream suppliers are invoked 0 times (test-plan #TP-42)
- [ ] 2.18 Add unit test: strict load fails: stale / absent / custom joins — copy harness glue from runtime/judo-runtime-core/judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit/util/ReferenceInjectorTest.java (JUnit 5 glue; new src/test sets per test-plan "New infra"); Triple: (a) generator mismatch; (b) no files; (c) custom joins / boot `load` / boot fails: (a) error names both identities; (b) "missing"; (c) "cannot be used with custom joins" (test-plan #TP-43)
