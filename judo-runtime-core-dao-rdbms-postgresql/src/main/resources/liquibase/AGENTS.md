# AGENTS.md — `judo-runtime-core-dao-rdbms-postgresql/src/main/resources/liquibase`

Liquibase change log for the PostgreSQL adapter: bootstraps null-aware `uuid` min/max aggregates and the root changelog that includes them.

| File | Purpose |
| --- | --- |
| `create-min-max.sql` | Liquibase formatted SQL installing null-aware `uuid` min/max aggregates. Creates plpgsql `min_uuid`/`max_uuid(uuid, uuid)` (both NULL→NULL, one NULL→the other, else smaller/larger) then aggregates `min(uuid)`/`max(uuid)` using them as `sfunc`/`combinefunc`, `parallel = safe`, `sortop = operator (<)`/`(>)`; every changeset starts with `drop function if exists ... cascade`. |
| `postgresql-init-changelog.xml` | Liquibase `databaseChangeLog` root (dbchangelog-3.4.xsd) that `<include file="create-min-max.sql" relativeToChangelogFile="true" />`. |