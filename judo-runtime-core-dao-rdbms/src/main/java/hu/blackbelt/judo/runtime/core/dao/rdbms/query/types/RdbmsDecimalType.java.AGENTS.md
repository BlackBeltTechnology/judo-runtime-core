# `RdbmsDecimalType.java`

Models SQL `DECIMAL(p,s)` type. Exports `DEFAULT_PRECISION` (100), `DEFAULT_SCALE` (30), `getPrecision()`, `getPrecisionOrDefault()`, `getScale()`, `getScaleOrDefault()`, `toSql()` rendering `DECIMAL(%d,%d)`, `toString()`.
Constructor `(Integer, Integer)` throws `IllegalArgumentException` when precision exceeds 100, scale exceeds 30, or precision < scale; nulls fall back to defaults via `Objects.requireNonNullElse`.