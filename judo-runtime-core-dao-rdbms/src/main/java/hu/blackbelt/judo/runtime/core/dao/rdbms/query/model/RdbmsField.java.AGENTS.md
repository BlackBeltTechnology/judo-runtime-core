# `RdbmsField.java`

Abstract root of the SQL-fragment model. Every column, constant, function, parameter,
navigation filter and result set in this package extends it, and every one of them gets
its aliasing and its numeric/length casting from here.

**Key exports**

- `@SuperBuilder @NoArgsConstructor public abstract class RdbmsField` with protected
  state `alias`, `Target target`, `@Getter EAttribute targetAttribute`.
- `public abstract String toSql(SqlConverterContext converterContext)` — the single
  method subclasses must implement.
- `getRdbmsAlias()` — returns `RdbmsAliasUtil.getTargetColumnAlias(target, alias)` when
  `target` is set, otherwise the bare `alias`.
- `protected getWithAlias(String sql, boolean includeAlias)` — appends
  `" AS " + getRdbmsAlias()` only when `includeAlias` is true **and** `alias` is non-null.
- `public static DomainConstraints getDomainConstraints(EAttribute)` — reads the ASM
  extension annotation named `constraints` via `AsmUtils.getExtensionAnnotationByName`
  and lifts `precision`, `scale`, `maxLength` details; returns `null` when the annotation
  is absent.
- `@Getter @Builder public static class DomainConstraints { Integer precision, scale,
  maxLength; }`.
- `protected cast(String sql, String typeName, EAttribute targetAttribute)`.

**Cast behaviour (`cast`)**

1. An explicit non-blank `typeName` wins: `CAST(<sql> AS <typeName>)`.
2. Otherwise the target attribute's domain constraints are converted to a SQL type —
   `precision` present → `RdbmsDecimalType(precision + scale, scale).toSql()`;
   `maxLength` present → `VARCHAR(<maxLength>)`; neither → no cast at all.
3. Decimal targets that exceed the floating-point envelope — `precision > 15`,
   `scale > 4`, or `scale == 0` — are not cast directly. `scale == 0` renders
   `CAST(FLOOR(CAST(x AS <default decimal>)) AS <sqlType>)`; otherwise the value is
   scaled by `1` followed by `scale` zeros, floored, and divided back, so that
   truncation (not rounding) is what reaches the target type.

**Contracts a caller can violate**

- `FLOATING_POINT_TYPE_MAX_PRECISION = 15` / `FLOATING_POINT_TYPE_MAX_SCALE = 4` are
  compile-time constants (`TODO JNG-4561` wants them configurable) — a model declaring
  wider decimals silently takes the floor/divide path.
- `getDomainConstraints` parses the annotation details with `Integer.parseInt`; a
  non-numeric `precision`/`scale`/`maxLength` detail throws `NumberFormatException` at
  SQL-build time, not at model load.
- A subclass that renders SQL without routing through `getWithAlias` loses the select-list
  alias the result-set mapper looks up, and one that skips `cast` emits an untyped
  expression the dialect may widen differently.
