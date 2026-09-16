# `ExpressionToFeatureConverter.java`

Abstract base for all converters.
Exports `convert(E, Context, FeatureTargetMapping)`, `applyMeasure`, `applyMeasureByUnit` (wraps feature in `MULTIPLE_DECIMAL` `Function` with unit-rate constant), `getConstraints` (precision/scale/maxLength `FunctionConstraint`s from extension annotations), `getSourceByVariableName`, `getBaseVariableName`, `getCollectionIterator` (creates `_iterator` on collection).
`getSourceByVariableName` throws `IllegalStateException` when the variable is absent from `context`.