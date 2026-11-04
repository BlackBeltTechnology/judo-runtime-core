# `JudoDefaultSpringConfiguration.java`

`@Configuration` assembling the full default RDBMS runtime from autowired parts.
It is what makes a judo Spring context a working application: one place that
wires DAO, dispatcher, query factory, resolvers, executors and validators.

## Autowired collaborators

Non-optional: `asmModel`, `asm2RdbmsTrace`, `rdbmsModel`, `expressionModel`,
`measureModel`, `DataSource`, `PlatformTransactionManager`, `Coercer`,
`DataTypeManager`, `IdentifierProvider`, `MetricsCollector`, `Export`,
`DispatcherFunctionProvider`, `OperationCallInterceptorProvider`, `Context`,
`Sequence`, `RdbmsParameterMapper`, `AuthenticationInterceptorProvider`.

Optional (`@Autowired(required = false)`): `OpenIdConfigurationProvider`,
`TokenIssuer filestoreTokenIssuer`, `TokenValidator filestoreTokenValidator` —
absent beans are tolerated; dispatcher then runs without OIDC/file-store tokens.

Context refresh fails without a `DataSource` and `PlatformTransactionManager`
bean: both are hard requirements, nothing here falls back for them.

## Exported beans (named exports)

- `getAccessManager()` → `DefaultAccessManager` from `asmModel` +
  `authenticationInterceptorProvider`.
- `getModifyStatementExecutor(...)` → `ModifyStatementExecutor`.
- `getQueryFactory()` → builds an `AsmJqlExtractor` over the ASM/measure
  resource sets with `JqlExpressionBuilderConfig.resolveOnlyCurrentLambdaScope=false`
  and constructs `QueryFactory`.
- `getRdbmsBuilder(...)` → `RdbmsBuilder` with `AncestorNameFactory`/
  `DescendantNameFactory` derived from every `EClass` in the ASM resource set.
- `getDAO(...)` → `RdbmsDAOImpl` with `optimisticLockEnabled=true` (hardwired).
- `getInstanceCollector(...)` → `RdbmsInstanceCollector` over a
  `NamedParameterJdbcTemplate(dataSource)`.
- `getRdbmsResolver(...)` → `RdbmsResolver`.
- `getSelectStatementExecutor(...)` → hardwired `chunkSize=1000`,
  `maximumRecursionCount=3`.
- `getTransformationTraceService()` → `TransformationTraceServiceImpl` plus the
  autowired `asm2RdbmsTrace`.
- `getActorResolver(DAO)` → `DefaultActorResolver` with
  `checkMappedActors=false`.
- `validatorProvider(DAO)` → `DefaultValidatorProvider(dao, identifierProvider, asmModel, context)`.
- `getPayloadValidator(...)` → `DefaultPayloadValidator` with
  `requiredStringValidatorOption=ACCEPT_NON_EMPTY` (hardwired).
- `getDispatcher(...)` → `DefaultDispatcher` with defaults:
  `metricsReturned=true`, `enableValidation=true`, `trimString=false`,
  `caseInsensitiveLike=false`.
- `getIdentifierSigner()` → `DefaultIdentifierSigner` with `secret=null` — no
  shared secret configured; ids are signed with a null-secret signer, so
  treat signed identifiers as non-sealed on this default wiring.
- `getVariableResolver()` → `DefaultVariableResolver` registering
  `SYSTEM.current_timestamp`, `SYSTEM.current_date`, `SYSTEM.current_time`
  suppliers and functions `ENVIRONMENT` (multiple=true), `SEQUENCE`, `REQUEST`
  (from `Context`). Those four namespaces are reserved by the default wiring.