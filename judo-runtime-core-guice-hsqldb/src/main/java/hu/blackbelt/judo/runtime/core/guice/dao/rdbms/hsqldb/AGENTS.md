# AGENTS.md — `judo-runtime-core-guice-hsqldb/src/main/java/hu/blackbelt/judo/runtime/core/guice/dao/rdbms/hsqldb`

Guice bindings for the HSQLDB (HyperSQL) RDBMS backend: module wiring, datasource, server, and sequence providers.

| File | Purpose |
| --- | --- |
| `HsqlDbConfigurationQualifier.java` | Grouping class owning Guice `@BindingAnnotation` qualifiers `HsqldbServerDatabaseName`, `HsqldbServerDatabasePath`, `HsqldbServerPort`; each `@Qualifier` with RUNTIME retention bounded to FIELD/PARAMETER/METHOD targets. |
| `HsqldbDataSourceProvider.java` | Guice `Provider<DataSource>`; `get()` builds JDBC URL from injected `Server` as `jdbc:hsqldb:hsql://localhost:<port>/<dbName>` or falls back to in-memory `jdbc:hsqldb:mem:<ts>;DB_CLOSE_DELAY=-1`, then wraps `JDBCPool` in `HikariDataSource`. |
| `HsqldbMapperFactoryProvider.java` | Guice `Provider<MapperFactory>` (rawtype) whose `get()` constructs `new HsqldbMapperFactory()` to back HSQLDB SQL-generation mappers. |
| `HsqldbRdbmsInitProvider.java` | Guice `Provider<RdbmsInit>`; builder-assembles `HsqldbRdbmsInit` with injected `SimpleLiquibaseExecutor` and liquibase model from `JudoModelLoader`, then calls `init.execute(dataSource)` so schema migrates as a side effect when Guice resolves `RdbmsInit`. |
| `HsqldbRdbmsParameterMapperProvider.java` | Guice `Provider<RdbmsParameterMapper>`; `get()` builds `HsqldbRdbmsParameterMapper` from injected `DataTypeManager` coercer, `JudoModelLoader` rdbmsModel, and `IdentifierProvider`. |
| `HsqldbRdbmsSequenceProvider.java` | Guice `Provider<Sequence>`; `get()` builds `HsqldbRdbmsSequence` from injected `DataSource` and optional `RdbmsSequenceStart`/`RdbmsSequenceIncrement`/`RdbmsSequenceCreateIfNotExists` injections defaulting to `1L`, `1L`, `true`. |
| `HsqldbServerProvider.java` | Guice `Provider<org.hsqldb.server.Server>`; starts a server using injected `HsqldbServerDatabaseName`/`HsqldbServerDatabasePath`/`HsqldbServerPort`, defaulting to timestamp name, temp-file path with `deleteOnExit`, and ephemeral port probed via `ServerSocket(0)`; `setNoSystemExit(true)` keeps server from killing the JVM. |
| `JudoHsqldbModule.java` | Guice `AbstractModule` wiring the HSQLDB runtime; `@Builder` ctor takes `JudoHsqldbModuleConfiguration` or per-field knobs; `configure()` binds `Dialect`, `Server`, `MapperFactory`, `RdbmsParameterMapper`, `DataSource`, `Sequence`, `RdbmsInit`, `PlatformTransactionManager` to the configured instance or its `*Provider` in `Singleton` when null. |
| `JudoHsqldbModuleConfiguration.java` | Value object feeding `JudoHsqldbModule`; static `DEFAULT` holds `@Builder.Default` values `runServer=false`, `databaseName="judo"`, `databasePath=new File(".", "judo.db")`, `port=31001`, remaining slots null. |