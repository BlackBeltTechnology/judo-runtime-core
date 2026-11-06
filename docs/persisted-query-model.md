# Persisted query model — modes, alignment, brittleness

Change: `persisted-query-model` (master change in the judo-ng superproject; module change in
`openspec/changes/persisted-query-model`).

The runtime used to rebuild the query model from JQL on every boot (about 7 s on RackInspect). The model build now saves it
as `<name>-query.model` next to the other generated model files (`<name>-asm.model`, `-measure.model`,
`-expression.model`, ...), and the runtime restores it at boot when it can.

## The decision

`QueryModelLoadOrBuild` (`judo-runtime-core-query`) is the only place that decides. Guice, Spring and the testkit fixture all
call it; nothing else opens the file.

| Situation | `auto` (default) | `load` | `build` |
| --- | --- | --- | --- |
| mode `build` | - | - | build, the file is never opened |
| custom joins configured | build (INFO) | fail | build |
| no `<name>-query.model` | build (INFO, no WARN) | fail ("missing") | build |
| unreadable, corrupt, unresolved references, any `Exception` or `Error` while restoring | build (WARN with the cause) | fail | build |
| `modelName`, `formatVersion` or generator stamp differs | build (WARN with both values) | fail | build |
| ASM, measure or expression digest differs | build (WARN naming the file) | fail | build |
| otherwise | restore (INFO "query model restored ... ms") | restore | build |

`build` is the container's existing build: JQL extraction with `resolveOnlyCurrentLambdaScope=false`, then `Asm2Query`.

## Choosing the mode

| Container | Option | Values |
| --- | --- | --- |
| Guice | `JudoDefaultModuleConfiguration.queryModelMode` / `JudoDefaultModule.builder().queryModelMode(...)` (a `configuration` object wins over the builder argument) | `auto` (default), `load`, `build` |
| Spring | property `judo.queryModel.mode` | `auto` (default), `load`, `build` |
| Testkit | `JudoRuntimeFixture.queryModelMode` (set before `prepare`), default from the system property `judo.queryModel.mode`, else `auto` | `auto`, `load`, `build` |

- Use `auto` in production: a missing or stale artifact only costs the old boot time.
- Use `load` in CI and in release verification: it turns "silently slow boot" into a failing boot, so a generator or model
  mismatch is caught where it is made.
- Use `build` as the rollback switch (or delete the artifact): no other code path changes.

A `QueryFactory` supplied in `JudoDefaultModuleConfiguration.queryFactory` bypasses the decision (Guice). An application with
its own `AsmModel` / `MeasureModel` / `ExpressionModel` beans (Spring) has no loader, so no artifact is located and the query
model is built.

## Where the artifact is looked for

`JudoModelLoader` (Guice and Spring variant) builds a locator for `<modelName>-query.model` in the same place the other model
files come from: a directory, a jar on the classpath (`model/`) or a URL. It checks only that the file exists, it never opens
it. While loading it also digests the complete bytes of the ASM, measure and expression files (SHA-256, drained to EOF).
Spring exposes the locator as the optional `PersistedQueryModel` bean (none without the file); there is still exactly one
`ExpressionModel` bean, the loader's. Guice binds no second `ExpressionModel` and no query-side model.

## Generator alignment (required)

The artifact carries the identity of the generator that saved it (`judo-tatami-asm2query` version and build stamp) and the
digests of the three files it was built from. Both must match the runtime's:

- Generate the model files and the runtime from the same `judo-tatami-base` build. Different generator identities are a
  mismatch even for the same model.
- Ship the three files and the artifact together. Replacing, re-saving or reformatting any of `-asm.model`,
  `-measure.model`, `-expression.model` after the artifact was saved changes its digest.

## `load` brittleness

`load` is exact on purpose and therefore fragile: any difference listed in the table above fails the boot, including an
unrelated rebuild of `judo-tatami-base` between generating the model and building the runtime, a model file touched by a
packaging step, or a custom join configured later. Do not use `load` where models and runtime are updated independently;
use `auto` there and watch for the WARN line `Persisted query model of <name> is not used, the query model is built: ...`.
