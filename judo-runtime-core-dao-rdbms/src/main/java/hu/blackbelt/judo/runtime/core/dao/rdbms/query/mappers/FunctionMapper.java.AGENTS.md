# `FunctionMapper.java`

Abstract dialect base that turns a logical `Function` node into an `RdbmsFunction` with a
`MessageFormat` SQL pattern and an ordered parameter list. Concrete dialect modules
(hsqldb, postgresql) extend it and override or add entries for dialect-specific SQL.

**Key exports**

- `abstract class FunctionMapper extends RdbmsMapper<Function>`.
- `FunctionMapper(@NonNull RdbmsBuilder)` — the only constructor; it populates the whole
  pattern table.
- `@Getter getFunctionBuilderMap()` → `LinkedHashMap<FunctionSignature,
  java.util.function.Function<FunctionContext, RdbmsFunction.RdbmsFunctionBuilder>>` —
  the single extension point subclasses mutate.
- `static class FunctionContext` (`@AllArgsConstructor @Builder`) with public fields
  `parameters` (`Map<ParameterName, RdbmsField>`), `builder`
  (`RdbmsFunction.RdbmsFunctionBuilder`), `function` (`Function`).
- `map(Function, RdbmsBuilderContext)` → `Stream<? extends RdbmsField>`.

**What the table covers**

Roughly 100 `FunctionSignature` entries registered in the constructor: boolean logic
(`NOT`, `AND`, `OR`, `XOR`, `IMPLIES`), comparisons (`EQUALS`, `NOT_EQUALS`,
`GREATER_THAN`, `GREATER_OR_EQUAL`, `LESS_THAN`, `LESS_OR_EQUAL`), arithmetic
(`ADD_*`, `SUBTRACT_*`, `MULTIPLE_*`, `DIVIDE_*`, `OPPOSITE_*`, `MODULO_*`), numeric
shaping (`INTEGER_ROUND`, `DECIMAL_ROUND`, `ABSOLUTE_NUMERIC`, `CEIL_NUMERIC`,
`FLOOR_NUMERIC`), string work (`LENGTH_STRING`, `LOWER_STRING`, `TRIM_STRING`,
`LEFT_TRIM_STRING`, `RIGHT_TRIM_STRING`, `LEFT_PAD`, …), and temporal construction
(`TO_TIMESTAMP`, `TO_TIME`) built by string-concatenating the `YEAR`/`MONTH`/`DAY`/
`HOUR`/`MINUTE`/`SECOND`/`MILLISECOND` parameters and `CAST`ing the result.

Integer variants frequently alias the decimal entry (`ADD_INTEGER` reuses the
`ADD_DECIMAL` lambda, `OPPOSITE_DECIMAL` reuses `OPPOSITE_INTEGER`), so replacing a
decimal entry in a subclass silently changes the integer one too unless it is re-put.

**Contracts a caller can violate**

- `map` resolves each `FunctionParameter` through `rdbmsBuilder.mapFeatureToRdbms(...)
  .findAny()`; a parameter that maps to no field throws
  `IllegalStateException("Rdbms field not found for parameter: <name>")`.
- A signature absent from `functionBuilderMap` throws
  `UnsupportedOperationException("Unsupported function: <signature>")` — a dialect that
  forgets an entry fails at query build time, not at wiring time.
- Parameters are collected into a `Map<ParameterName, RdbmsField>`; two function
  parameters sharing a `ParameterName` collide in `Collectors.toMap`.
- Patterns are `MessageFormat` strings: literal single quotes must be doubled (`''`) as
  the temporal entries do, and `{n}` indices must match the `parameters(List.of(...))`
  order.
- Decimal casts default to `new RdbmsDecimalType().toSql()`, so dialects with a different
  decimal type must override the affected entries.
