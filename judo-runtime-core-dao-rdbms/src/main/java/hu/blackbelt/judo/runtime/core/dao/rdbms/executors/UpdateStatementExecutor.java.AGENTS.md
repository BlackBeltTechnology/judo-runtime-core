# `UpdateStatementExecutor.java`

Executes `UpdateStatement`s in two SQL steps: a meta update that increments `VERSION = VERSION + 1` and writes `UPDATE_*` audit columns, then the attribute update.
Optimistic locking is enforced in the WHERE clause (`AND VERSION = :__version`) whenever the statement carries a version; both statements `checkState(count == 1, "There is illegal state, no records updated")`, so a stale version surfaces as a failure, not a silent no-op.