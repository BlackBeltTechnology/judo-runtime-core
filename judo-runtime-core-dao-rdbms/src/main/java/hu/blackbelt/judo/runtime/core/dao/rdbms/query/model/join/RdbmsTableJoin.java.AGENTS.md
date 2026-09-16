# `RdbmsTableJoin.java`

Joins a plain physical table. `@SuperBuilder` over `@NonNull String tableName` and an optional `RdbmsTableJoin rdbmsPartnerTable`.
When `rdbmsPartnerTable` is set it wins over the inherited `partnerTable` (logging a WARN if both are set); the condition becomes `<prefix><partner.alias>.<partner.columnName> = <prefix><alias>.<columnName>` plus any `onConditions` ANDed in.
Otherwise it delegates to `RdbmsJoin.getJoinCondition`.