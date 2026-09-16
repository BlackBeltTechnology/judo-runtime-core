# `JudoHsqldbSpringConfiguration.java`

Spring auto-configuration providing the HSQLDB-backed database stack for
`judo-runtime-core` apps.

Exports (all `@Bean`):

- `getHsqlsbDialect()` — `HsqldbDialect`.
- `getHsqlsbSequence()` — `HsqldbRdbmsSequence` built from autowired `DataSource` with start=1, increment=1, createIfNotExists=true.
- `getHsqlsbRdbmsParameterMapper(IdentifierProvider, Dialect, Coercer, RdbmsModel)` — `HsqldbRdbmsParameterMapper`.
- `getHsqlsbMapperFactory()` — `HsqldbMapperFactory`.

Contracts a caller can violate:

- `@ConditionalOnExpression("'${spring.datasource.url}'.contains('hsqldb')")` — beans register only when `spring.datasource.url` references an hsqldb URL; a non-hsqldb datasource leaves the stack absent.
- Beans autowire `DataSource` and `PlatformTransactionManager`; applicants must provide those beans or the configuration fails context startup.
- Sequence parameterization is hard-coded (start/increment/createIfNotExists); custom sequences need a replacement `Sequence` bean.