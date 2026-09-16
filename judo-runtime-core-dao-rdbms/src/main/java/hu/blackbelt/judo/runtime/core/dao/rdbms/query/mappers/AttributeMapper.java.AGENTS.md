# `AttributeMapper.java`

Maps `Attribute` to `RdbmsColumn` with `columnName` from `rdbmsBuilder.getColumnName(sourceAttribute)` and `sourceDomainConstraints`.
Detects inherited attributes by comparing node type with `sourceAttribute.getEContainingClass()`; on mismatch it registers the container in `builderContext.getAncestors()` and sets `partnerTablePostfix` to the ancestor alias, without which the ancestor JOIN is never emitted.