# AGENTS.md — `judo-runtime-core-dao-rdbms-postgresql/src/test/resources`

Test logging configuration shared by PostgreSQL adapter tests.

| File | Purpose |
| --- | --- |
| `logback.xml` | Logback config for PostgreSQL tests: single `CONSOLE` appender with `PatternLayout` `%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n`, root level `warn`, `LevelChangePropagator` context listener with `resetJUL=true` syncing Jul levels. |