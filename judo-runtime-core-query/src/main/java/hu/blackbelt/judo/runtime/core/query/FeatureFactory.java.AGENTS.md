# `FeatureFactory.java`

Registry mapping expression types to `ExpressionToFeatureConverter`s; converts `Expression` → `Feature`. Exports
`convert(Expression, Context, FeatureTargetMapping)`; constructor (`JoinFactory`, `AsmModelAdapter`, `Coercer`, `MeasureProvider`)
wires ~90 converters from `...query.feature.*`. Throws `IllegalStateException` for expressions without a registered converter;
resolves by first converter key `isAssignableFrom` the expression class. Non-null `targetMapping` is appended to
the produced feature's `targetMappings`.