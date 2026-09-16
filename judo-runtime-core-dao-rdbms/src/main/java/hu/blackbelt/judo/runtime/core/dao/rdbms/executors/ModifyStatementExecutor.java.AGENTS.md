# `ModifyStatementExecutor.java`

Public façade: `executeStatements(NamedParameterJdbcTemplate, Collection<Statement>)` builds every package-private executor and runs them in a fixed precedence — exists-validation → remove/add consistency checks → remove references → deletes → unique-attribute checks → inserts → updates → reference updates → add references.
Callers must pass one whole transactional batch; the ordering, not the caller, resolves inter-statement dependencies.