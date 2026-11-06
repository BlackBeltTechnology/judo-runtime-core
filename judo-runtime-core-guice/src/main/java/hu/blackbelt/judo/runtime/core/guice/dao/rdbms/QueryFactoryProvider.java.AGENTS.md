# `QueryFactoryProvider.java`

Guice `Provider<QueryFactory>`. `get()` builds `JqlExpressionBuilderConfig` with
`resolveOnlyCurrentLambdaScope=false`, instantiates `AsmJqlExtractor` over asm+measure
resource sets at URI `expr:<asmName>`, then constructs `QueryFactory` from extracted
expressions. Optional `@JudoConfigurationQualifiers.QueryFactoryCustomJoinDefinitions`
map of `EReference`→`CustomJoinDefinition`, defaulted via `requireNonNullElse` to empty
`ConcurrentHashMap`.


## Persisted query model (See change: persisted-query-model)

`get()` now calls `QueryModelLoadOrBuild` (`judo-runtime-core-query`) with the loader's `AsmModel` / `MeasureModel` / `ExpressionModel`, their digests, `getPersistedQueryModel()`, the custom joins and the optional `@JudoConfigurationQualifiers.QueryModelMode` string (`auto` when not bound). Its build supplier is the extraction above, unchanged (`resolveOnlyCurrentLambdaScope=false`, `AsmJqlExtractor`), and runs only when the decision falls back; custom joins force the build. Created eagerly at injector creation through the eager `Dispatcher`, as before; a `JudoDefaultModuleConfiguration.queryFactory` still bypasses the provider. Tests: `QueryFactoryProviderTest`, `JudoDefaultModuleQueryModelTest`.
