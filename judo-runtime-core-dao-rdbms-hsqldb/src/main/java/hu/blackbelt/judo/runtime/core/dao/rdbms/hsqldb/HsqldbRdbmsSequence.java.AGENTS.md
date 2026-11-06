# `HsqldbRdbmsSequence.java`

Implements `Sequence<Long>` over HSQLDB `NEXT VALUE FOR` / `CURRENT VALUE FOR` via `NamedParameterJdbcTemplate`.
Exports `getNextValue(String)` and `getCurrentValue(String)`; names are sanitised by `replaceAll("[^a-zA-Z0-9_]", "_")`, so names differing only in punctuation collide.
`createIfNotExists` (default true) issues `CREATE SEQUENCE IF NOT EXISTS` on every call, so `start`/`increment` apply only at first creation.