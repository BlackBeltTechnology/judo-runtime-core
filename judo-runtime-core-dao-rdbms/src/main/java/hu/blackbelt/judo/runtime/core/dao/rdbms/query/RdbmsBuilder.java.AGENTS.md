# `RdbmsBuilder.java`

Central dispatcher of the RDBMS query layer: every logical `hu.blackbelt.judo.meta.query`
feature or JOIN node passes through here on its way to an `RdbmsField` / `RdbmsJoin`.

**Key exports**

- `@Builder RdbmsBuilder(RdbmsResolver, RdbmsParameterMapper, IdentifierProvider, Coercer,
  AncestorNameFactory, DescendantNameFactory, VariableResolver, RdbmsModel, AsmModel,
  MapperFactory, Dialect)` — every parameter is `@NonNull`.
- `mapFeatureToRdbms(ParameterType, RdbmsBuilderContext)` → `Stream<RdbmsField>`.
- `getTableName(EClass)` / `getColumnName(EAttribute)` — delegate to `RdbmsResolver`
  (`rdbmsTable(...).getSqlName()`, `rdbmsField(...).getSqlName()`).
- `getAncestorPostfix(EClass)` / `getDescendantPostfix(EClass)` — alias factory passthrough.
- `processJoin(JoinProcessParameters)` → `List<RdbmsJoin>`.
- `addAncestorJoins(Collection<RdbmsJoin>, Node, RdbmsBuilderContext)`,
  `addFilterJoinsAndConditions(FilterJoinProcessorParameters, RdbmsBuilderContext)`,
  `processSimpleJoin(SimpleJoinProcessorParameters)`.
- `getConstantFields()` → `ThreadLocal<Map<String, Collection<? extends RdbmsField>>>`.
- Lombok `@Getter` exposes `rdbmsResolver`, `parameterMapper`, `identifierProvider`,
  `coercer`, `constantCounter`, both name factories, `variableResolver`, `rdbmsModel`,
  `asmModel`, `dialect`.

**Construction**

The constructor owns the join-processor graph: it builds `SimpleJoinProcessor`,
`AncestorJoinsProcessor`, `CastJoinProcessor`, `ContainerJoinProcessor`,
`CustomJoinProcessor`, `FilterJoinProcessor` and `SubSelectJoinProcessor` eagerly, and
pulls the mapper table from `mapperFactory.getMappers(this)` — so the factory receives a
partially initialised builder and must not call back into it during `getMappers`.

**Contracts a caller can violate**

- The RDBMS model must contain a `Rules` element in its resource contents; otherwise the
  constructor throws `IllegalArgumentException("Rules not found in RDBMS model")`.
- `processJoin` recognises only `ReferencedJoin`, `ContainerJoin`, `CastJoin`,
  `SubSelectJoin` and `CustomJoin`; any other `Node` throws
  `IllegalStateException("Invalid JOIN")`.
- `mapFeatureToRdbms` selects mappers by `isAssignableFrom` on the value's runtime class
  and flat-maps every match — a feature type matching two registered mappers yields the
  fields of both.
- `constantCounter` (`AtomicInteger`) and the `CONSTANT_FIELDS` `ThreadLocal` carry
  per-statement state: constants staged on one thread are invisible to another, and the
  counter is never reset, so aliases are unique per builder instance, not per query.
