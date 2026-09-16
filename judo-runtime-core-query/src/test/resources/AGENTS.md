# AGENTS.md — `test/resources` (judo-runtime-core-query)

| File | Purpose |
| --- | --- |
| `logback.xml` | Test logback config. Console `PatternLayout` appender `CONSOLE`; root logger at `warn`; `LevelChangePropagator` with `resetJUL=true` bridges JUL levels. Tests emit warn-level logs unless a caller overrides `<root>` level. |