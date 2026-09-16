# AGENTS.md — `judo-runtime-core-query/src/main/java/hu/blackbelt/judo/runtime/core/query/feature/aggregated`

Aggregation converters: reduce a collection expression to a single value by
building a `SubSelect` over the collection element type. All extend
`ExpressionToFeatureConverter` from the parent `feature` package.

| File | Purpose |
| --- | --- |
| `ConcatenateCollectionToFeatureConverter.java` | Stub for `ConcatenateCollection`: `convert` always throws `UnsupportedOperationException("Not supported yet")`. |
| `CountExpressionToFeatureConverter.java` | Converts `CountExpression` into a `SubSelect` feature counting the collection: derives iterator variable via `getCollectionIterator`, sub-selects element `EClass` with `getNextSubSelectAlias`, targets a `ObjectVariableReference` built with `newObjectVariableReferenceBuilder`; joins via `JoinFactory`. |
| `DateAggregatedExpressionToFeatureConverter.java` | Converts `DateAggregatedExpression` (min/max over a date collection) into a `SubSelect` feature: sub-selects the iterator `EClass` via `getNextSubSelectAlias`, converts navigations with `joinFactory.convertNavigationToJoins`, sets the aggregate as embedded select. |
| `DecimalAggregatedExpressionToFeatureConverter.java` | Converts `DecimalAggregatedExpression` (sum/avg/min/max over a decimal collection) into a `SubSelect` feature over the iterator `EClass`; joins via `JoinFactory` and embeds the aggregate select. |
| `IntegerAggregatedExpressionToFeatureConverter.java` | Converts `IntegerAggregatedExpression` into a `SubSelect` feature over the iterator `EClass`; joins via `JoinFactory`, embeds the aggregate. |
| `ObjectSelectorExpressionToFeatureConverter.java` | Converts `ObjectSelectorExpression` into a `SubSelect` feature selecting the object element: clones `context`, sub-selects the element `EClass`, targets the iterator's `ObjectVariableReference`; joins via `JoinFactory`. |
| `StringAggregatedExpressionToFeatureConverter.java` | Converts `StringAggregatedExpression` (min/max/concatenate over a string collection) into a `SubSelect` feature over the iterator `EClass`; joins via `JoinFactory`, embeds the aggregate. |
| `TimeAggregatedExpressionToFeatureConverter.java` | Converts `TimeAggregatedExpression` into a `SubSelect` feature over the iterator `EClass`; joins via `JoinFactory`, embeds the aggregate. |
| `TimestampAggregatedExpressionToFeatureConverter.java` | Converts `TimestampAggregatedExpression` into a `SubSelect` feature over the iterator `EClass`; joins via `JoinFactory`, embeds the aggregate. |