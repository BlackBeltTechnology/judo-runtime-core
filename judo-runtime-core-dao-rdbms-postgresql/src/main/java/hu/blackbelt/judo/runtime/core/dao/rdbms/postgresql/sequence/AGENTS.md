# AGENTS.md — `judo-runtime-core-dao-rdbms-postgresql/src/main/java/hu/blackbelt/judo/runtime/core/dao/rdbms/postgresql/sequence`

PostgreSQL-backed sequence provider for the dispatcher API, implemented on native `CREATE SEQUENCE` / `NEXTVAL` / `CURRVAL`.

| File | Purpose |
| --- | --- |
| `PostgresqlRdbmsSequence.java` | PostgreSQL `Sequence<Long>` backed by native `CREATE SEQUENCE`. `@Builder` ctor requires `@NonNull DataSource`; `start`/`increment` fall back to `Sequence.DEFAULT_START`/`DEFAULT_INCREMENT`, `createIfNotExists` defaults true. Sanitizes name via `replaceAll("[^a-zA-Z0-9_]", "_")`, runs `CREATE SEQUENCE IF NOT EXISTS` then `NEXTVAL`/`CURRVAL` through `NamedParameterJdbcTemplate`. |