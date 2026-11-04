# `RdbmsJoinComparator.java`

Orders joins so a join is emitted after everything it references. `RdbmsJoinComparator(Collection<RdbmsJoin>)` snapshots the original order; `compare` prefers `aliasToCompareWith` over `alias`, returns -1/1 when one side's `joinConditionTableAliases` contains the other's alias, else falls back to `originalOrder.indexOf` difference.
Throws `IllegalArgumentException` on a null join or a null/blank alias.
Dependencies must be recorded (i.e. `toSql`/`getJoinCondition` already called) before sorting; the relation is not transitive — best-effort ordering, not a topological sort.