# Archive notes — persisted-query-model

Do these by hand when the change is archived (`openspec archive`); the delta merge cannot do them.

- The `Architecture` prose of the store specs `openspec/specs/query/spec.md` and `openspec/specs/expression/spec.md` still
  describes the moved classes (`FeatureFactory`, `JoinFactory`, `Context`, `Constants`, the feature converters,
  `EntityTypeExpressions` and the expression collector/bindings of `judo-runtime-core-expression`). Rewrite it:
  - `query`: `QueryFactory` builds through `Asm2Query` (`judo-tatami-asm2query`, judo-tatami-base) or restores a
    persisted query model (`QueryFactory.restore`); `QueryModelLoadOrBuild` decides (modes `auto`, `load`, `build`);
    the builder types live in `hu.blackbelt.judo.tatami.asm2query.*`.
  - `expression`: the capability is removed with the module; point to `asm2query`.
- `docs/persisted-query-model.md` is the user-facing description of the modes; link it from the `guice`, `spring` and
  `guice-testkit` store specs' prose if they gain an Architecture section.
