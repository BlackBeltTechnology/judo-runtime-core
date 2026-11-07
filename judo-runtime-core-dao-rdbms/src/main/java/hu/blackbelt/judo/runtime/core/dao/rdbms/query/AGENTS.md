# AGENTS.md — `judo-runtime-core-dao-rdbms/src/main/java/hu/blackbelt/judo/runtime/core/dao/rdbms/query`

Translation layer turning `hu.blackbelt.judo.meta.query` logical query nodes into RDBMS
SQL fragments: alias factories, the central `RdbmsBuilder` dispatcher, and the context
object threaded through every mapper, join processor and translator below.

| File | Purpose |
| --- | --- |
| `AncestorNameFactory.java` | Mints SQL table aliases for ancestor `EClass` holders. Exports `AncestorNameFactory(Stream<EClass>)`, `getAncestorPostfix(EClass)` formatting `_a{0,number,00}`. Indexes live in a `ConcurrentHashMap` seeded by the constructor; unknown classes get a new index on demand, so alias numbering follows first-call order. |
| `DescendantNameFactory.java` | Mints SQL table aliases for descendant `EClass` holders. Exports `DescendantNameFactory(Stream<EClass>)`, `getDescendantPostfix(EClass)` formatting `_d{0,number,00}`. Same lazy-index contract as the ancestor factory; `_d` alias space must not collide with `_a`. |
| `RdbmsBuilder.java` | Central dispatcher converting query metamodel nodes to RDBMS fields and joins. → see `RdbmsBuilder.java.AGENTS.md` |
| `RdbmsBuilderContext.java` | Carries per-query state to builder, mappers and join processors. Lombok `@Builder(toBuilder = true)`/`@Getter` over `rdbmsBuilder`, `ancestors`, `descendants`, `parentIdFilterQuery`, `queryParameters`. `rdbmsBuilder` is `@NonNull`; the default `ancestors`/`descendants` maps are shared by every processor holding the context — derive variants via `toBuilder()`. |
