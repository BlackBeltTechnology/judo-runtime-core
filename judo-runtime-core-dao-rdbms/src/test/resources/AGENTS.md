# AGENTS.md — `judo-runtime-core-dao-rdbms/src/test/resources`

Test logging configuration shared by rdbms DAO tests.

| File | Purpose |
| --- | --- |
| `logback.xml` | Logback config for rdbms DAO tests: single `CONSOLE` appender with `PatternLayout` `%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n`, root level `warn`, `LevelChangePropagator` context listener with `resetJUL=true` syncing Jul levels. |