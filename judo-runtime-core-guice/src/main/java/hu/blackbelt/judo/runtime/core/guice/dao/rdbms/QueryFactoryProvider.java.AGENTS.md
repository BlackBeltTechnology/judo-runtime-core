# `QueryFactoryProvider.java`

Guice `Provider<QueryFactory>`. `get()` builds `JqlExpressionBuilderConfig` with
`resolveOnlyCurrentLambdaScope=false`, instantiates `AsmJqlExtractor` over asm+measure
resource sets at URI `expr:<asmName>`, then constructs `QueryFactory` from extracted
expressions. Optional `@JudoConfigurationQualifiers.QueryFactoryCustomJoinDefinitions`
map of `EReference`→`CustomJoinDefinition`, defaulted via `requireNonNullElse` to empty
`ConcurrentHashMap`.