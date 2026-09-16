# `JudoPostgresqlSpringConfiguration.java`

Spring `@Configuration` wiring PostgreSQL-backed beans.
Exports `getPostgresqlDialect()`, `getPostgresqlSequence()`, `getPostgresqlRdbmsParameterMapper(...)`, `getPostgresqlMapperFactory()`.
Activates only when `${spring.datasource.url}` contains `postgresql` via `@ConditionalOnExpression`.
Consumers auto-wire bean types `PostgresqlDialect`, `Sequence`, `RdbmsParameterMapper`, `MapperFactory`; non-postgresql URL leaves them absent.