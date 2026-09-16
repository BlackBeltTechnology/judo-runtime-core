# AGENTS.md — `judo-runtime-core-spring-postgresql/src/main/java/hu/blackbelt/judo/runtime/core/spring/postgresql`

| File | Purpose |
| --- | --- |
| `JudoPostgresqlSpringConfiguration.java` | Spring `@Configuration` wiring PostgreSQL-backed beans; exports `getPostgresqlDialect()`, gated by `@ConditionalOnExpression` → see `JudoPostgresqlSpringConfiguration.java.AGENTS.md` |