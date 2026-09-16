# AGENTS.md — `judo-runtime-core-dao-rdbms-liquibase/src/main/java/hu/blackbelt/judo/runtime/core/dao/rdbms/liquibase`

Liquibase changelog execution over a `DataSource` — either from a classpath resource or from an in-memory `LiquibaseModel`.

| File | Purpose |
| --- | --- |
| `SimpleLiquibaseExecutor.java` | Applies a Liquibase changelog against a `DataSource` → see `SimpleLiquibaseExecutor.java.AGENTS.md` |
| `StreamResourceAccessor.java` | Liquibase `AbstractResourceAccessor` backed by an in-memory `Map<String, InputStream>`. `@RequiredArgsConstructor` ctor with `@NonNull streams`; `openStreams` matches escaped key to escaped path, `list`/`describeLocations` expose escaped keys, `escape` maps chars outside alphabetic/digit/`-_.` to `_`. Lookup compares escaped forms, so separator-bearing raw paths still match. |