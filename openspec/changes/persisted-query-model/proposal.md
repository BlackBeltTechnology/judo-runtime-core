## Why

The Query model is rebuilt from JQL on every runtime boot (about 7 s on RackInspect). The master change
`openspec/changes/persisted-query-model` in the judo-ng superproject persists it as `<name>-query.model` at model
build time. In `judo-runtime-core` (rank 8) the runtime entry point restores the model, one shared decision
(`QueryModelLoadOrBuild`) picks restore or build, and the Guice and Spring containers digest and locate the artifact.
The query builder moves to `judo-tatami-base`, so `judo-runtime-core-expression` goes away.

## What Changes

- `judo-runtime-core-query` keeps `QueryFactory` and `CustomJoinDefinition`, and gains `QueryModelLoadOrBuild`.
  The build path delegates to `Asm2Query` (`judo-tatami-asm2query`); the new restore path wraps a `QueryIndex`
  from a loaded `QueryModel`.
- New `QueryModelLoadOrBuild`: the decision table over ASM, measure and expression digests, stamps, custom joins and
  failures, with the modes `auto` (default), `load` and `build`.
- **BREAKING**: `FeatureFactory`, `JoinFactory`, `Context`, `Constants`, the feature converters and
  `EntityTypeExpressions` leave `hu.blackbelt.judo.runtime.core.query.*` for
  `hu.blackbelt.judo.tatami.asm2query.*`. `QueryFactory.dataExpressionToFeature(..., Context, ...)` and
  `getEntityTypeExpressionsMap()` change signature. Internal callers (`SelectStatementExecutor`, `FunctionMapper`,
  `AttributeSelectorTranslator`) are updated.
- The `coercer` argument of `QueryFactory` stays, is `@Deprecated` and ignored.
- **BREAKING**: module `judo-runtime-core-expression` is removed (reactor, BOM, POMs); its collector, bindings and
  skill package move to `judo-tatami-asm2query`. Every requirement of the `expression` capability is REMOVED.
- Guice: `JudoModelLoader` digests ASM, measure and expression model while loading and builds a
  `PersistedQueryModel` locator; `QueryFactoryProvider` uses `QueryModelLoadOrBuild`; new `queryModelMode` in
  `JudoDefaultModuleConfiguration` and the `JudoDefaultModule` builder copy. No second `ExpressionModel` binding.
- Spring: the `JudoModelLoader` variant does the same; `JudoModelConfiguration` exposes an optional
  `PersistedQueryModel` bean; the `QueryFactory` bean uses `QueryModelLoadOrBuild` with `judo.queryModel.mode`.
- `judo-runtime-core-guice-testkit`: the fixture obtains its `QueryFactory` through `QueryModelLoadOrBuild`
  (mode configurable).
- Docs: mode documentation (generator alignment, `load` brittleness) and `AGENTS.md` rows.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `query`: restore path, `QueryModelLoadOrBuild`, modes; moved types removed from the module; deprecated `coercer`.
- `expression`: every requirement REMOVED (moved to `asm2query` in `judo-tatami-base`).
- `guice`: model loader digests and locator, `QueryFactoryProvider` decision, `queryModelMode`.
- `spring`: model loader digests and locator, optional `PersistedQueryModel` bean, `judo.queryModel.mode`.
- `guice-testkit`: fixture builds its `QueryFactory` through the shared decision.

## Impact

- **Code**: `judo-runtime-core-query` (reduced, `QueryFactory`, `QueryModelLoadOrBuild`);
  `judo-runtime-core-expression` (deleted); `judo-runtime-core-dao-rdbms` (moved-type imports);
  `judo-runtime-core-guice` (`JudoModelLoader`, `QueryFactoryProvider`, `JudoDefaultModule`,
  `JudoDefaultModuleConfiguration`); `judo-runtime-core-spring` (`JudoModelLoader`, `JudoModelConfiguration`,
  `JudoDefaultSpringConfiguration`); `judo-runtime-core-guice-testkit` (`JudoRuntimeFixture`).
- **POMs**: reactor, `judo-runtime-core-dependencies` BOM, dao-rdbms, dao-rdbms-hsqldb, dao-rdbms-postgresql, query.
- **Dependencies**: adds `judo-tatami-asm2query` (judo-tatami-base, rank 7) and, through it, `judo-meta-query`
  (rank 4). Module rank stays 8.
- **Compatibility**: with no query artifact present, behaviour is unchanged (build path). Downstream code using the
  moved types must recompile against the new package.
- **Rollback**: `queryModelMode=build` / `judo.queryModel.mode=build`, or delete the artifact. Reverting the
  module restores the old packages; no database or schema impact.
