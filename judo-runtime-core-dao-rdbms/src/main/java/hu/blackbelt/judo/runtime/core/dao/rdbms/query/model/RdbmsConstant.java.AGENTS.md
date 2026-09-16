# `RdbmsConstant.java`

Renders a `RdbmsParameterMapper.Parameter` as an **inline SQL literal** rather than a
JDBC bind variable — used where the dialect or the query shape forbids a parameter
marker (DDL-ish fragments, constants inside aggregate sub-selects).

**Key exports**

- `@SuperBuilder RdbmsConstant extends RdbmsField` over
  `RdbmsParameterMapper.Parameter parameter` and `int index`.
- `toSql(SqlConverterContext)` — the whole behaviour of the file.

**Rendering rules, keyed on `parameter.getSqlType()`**

- `VARCHAR` / `CHAR` / `NVARCHAR` / `BINARY` / `OTHER` — coerced to `String`, wrapped in
  single quotes, embedded `'` doubled. A null value renders the four characters `NULL`
  *inside* the quotes, i.e. `'NULL'`, not SQL `NULL`.
- `BIGINT` / `DECIMAL` / `DOUBLE` / `FLOAT` / `INTEGER` / `SMALLINT` / `TINYINT` /
  `REAL` / `NUMERIC` — coerced to `BigDecimal`, printed with `toPlainString()`; null
  renders unquoted `NULL`.
- `TIMESTAMP` / `TIMESTAMP_WITH_TIMEZONE` — `timestampToSql` coerces to `String`, drops a
  trailing `Z`, replaces `T` with a space, and emits
  `CAST('<value>' AS <parameter.getRdbmsTypeName()>)`. A null or blank raw value produces
  the literal text `CAST('NULL' AS …)`.
- `TIME` / `DATE` — `CAST('<value>' AS <rdbmsTypeName>)`, null yielding `CAST(NULL AS …)`.
- `BOOLEAN` / `BIT` — `(1 = 1)` for true, `(1 = 0)` for false, `NULL` for null. No dialect
  boolean literal is used.
- anything else — logs `"Unsupported constant type: {}, replaced with SQL named
  parameter"` at WARN and degrades to a bound parameter named `c<index>` registered on
  `converterContext.getSqlParameters()`, then `cast(…)` to the target attribute type.

**Contracts a caller can violate**

- `index` must be unique within one statement: the fallback path binds `c<index>` on the
  shared `MapSqlParameterSource`, so a duplicate index silently overwrites the earlier
  value. `RdbmsBuilder`'s `constantCounter` is what normally guarantees this.
- Values reach SQL text unescaped except for the quote-doubling on string types — the
  coercer output for non-string types is trusted verbatim.
- The final result passes through `RdbmsField.getWithAlias(sql, includeAlias)`, so an
  alias is appended only when the context sets `includeAlias`.
