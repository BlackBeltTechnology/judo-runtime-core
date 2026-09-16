# `JudoPostgresqlModule.java`

Guice `AbstractModule` that wires the whole PostgreSQL RDBMS DAO stack in one place: dialect, transaction manager, data source, mapper factory, parameter mapper, sequence and Liquibase schema init. The switch `JudoPostgresqlModuleBuilder` lets callers supply ready-built services or plain connection fields and get the module's default providers for everything else.

**Key exports**

- `public class JudoPostgresqlModule extends AbstractModule` with `@Getter JudoPostgresqlModuleConfiguration configuration`.
- static nested `JudoPostgresqlModuleBuilder` — per-field defaults copied from `JudoPostgresqlModuleConfiguration.DEFAULT` (`host`, `port`, `user`, `password`, `databaseName`, `poolSize`, plus nullable service slots for `PlatformTransactionManager`, `MapperFactory`, `RdbmsParameterMapper`, `DataSource`, `Sequence`, `RdbmsInit`).
- `@Builder private JudoPostgresqlModule(JudoPostgresqlModuleConfiguration, host, port, user, password, databaseName, poolSize, platformTransactionManager, mapperFactory, rdbmsParameterMapper, dataSource, sequence, rdbmsInit)` — a non-null `configuration` wins; otherwise the fields are folded into a new `JudoPostgresqlModuleConfiguration`.
- `protected void configure()` — calls `configureDialect()`, `configurePlatformTransactionManager()`, `configureOptions()`, `configureDataSource()`, `configureMapperFactory()`, `configureRdbmsInit()`, `configureRdbmsParameterMapper()`, `configureSequence()`.
- `configureDialect()` binds `Dialect` to a `PostgresqlDialect` instance.
- `configureOptions()` binds the five `@PostgresqlConfiguration` annotations (`PostgresqlPort`, `PostgresqlHost`, `PostgresqlUser`, `PostgresqlPassword`, `PostgresqlDatabaseName`) as literal `Integer`/`String` instances from the configuration.
- Each service method (`configurePlatformTransactionManager`, `configureMapperFactory`, `configureRdbmsParameterMapper`, `configureDataSource`, `configureSequence`, `configureRdbmsInit`) binds the configured instance if non-null, else the matching `...Provider` (`PlatformTransactionManagerProvider`, `PostgresqlMapperFactoryProvider`, `PostgresqlRdbmsParameterMapperProvider`, `PostgresqlDataSourceProvider`, `PostgresqlRdbmsSequenceProvider`, `PostgresqlRdbmsInitProvider`) in `Singleton` scope.

**Contracts a caller can violate**

- Configuring a service on the builder (e.g. a custom `DataSource`) silently overrides the module's default provider for that binding only — other services keep their providers, so a caller must set all slots it expects customised.
- Passing a non-null `JudoPostgresqlModuleConfiguration` while also setting builder fields is ambiguous: the configuration wins and the fields are ignored.
- The `@PostgresqlConfiguration`-annotated option values are bound as fixed instances; external modules cannot rebind them after installation.
- `configureRdbmsInit` binds the `PostgresqlRdbmsInitProvider`, whose `get()` executes Liquibase against the `DataSource` at first provision — installing this module means schema init may run as a side effect of the first injection.