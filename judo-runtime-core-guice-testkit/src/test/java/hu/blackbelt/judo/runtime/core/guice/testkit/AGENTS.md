# AGENTS.md — `judo-runtime-core-guice-testkit/src/test/java/hu/blackbelt/judo/runtime/core/guice/testkit`

| File | Purpose |
| --- | --- |
| `JudoTestAnnotationTest.java` | Tests `@JudoTest` annotation: positive/negative scenarios, transaction behavior, DataSource modes `BY_CLASS`/`BY_METHOD`/`SINGLETON`, transaction handling strategies. `@Disabled` — requires real JUDO model named `'test'` absent from test env; stays inert until model provided. |
| `JudoTestFrameworkTest.java` | Tests framework parts needing no real JUDO model: DataSource creation/management via `JudoDatasourceFixture`/`JudoTestExtension`, transaction manager availability, connectivity, error handling, resource cleanup. Export: nested suite `DataSourceFixtureTests`. |
| `logback.xml` | Logback config: `CONSOLE` appender with `PatternLayout` `%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n`, root level `error`. Debug output never reaches console unless root level raised. |