# AGENTS.md — `judo-runtime-core-query/src/main/java/hu/blackbelt/judo/runtime/core/query/feature`

Converts judo meta `Expression` nodes into query-model `Feature` nodes. One
converter per expression type; all extend the abstract `ExpressionToFeatureConverter`
in this directory and are dispatched by the `FeatureFactory` in the parent package.

| File | Purpose |
| --- | --- |
| `AbsoluteToFeatureConverter.java` | Converts numeric `AbsoluteExpression` into a query `Function` feature via `newFunctionBuilder`. Registers built features on `context`. |
| `AsStringToFeatureConverter.java` | Converts `AsString` expression into a string `Function` feature with a converted operand parameter; builds leading `IntegerConstant` for the target length via `newIntegerConstantBuilder`, guarded by `checkState`. |
| `AttributeToFeatureConverter.java` | Converts `AttributeSelector` (decimal/integer attribute reads) into an `Attribute` feature via `newAttributeBuilder`. Resolves attributes through `modelAdapter` and builds joins via `JoinFactory` when the selector navigates. |
| `CapitalizeToFeatureConverter.java` | Converts `Capitalize` string expression into a `Function` feature with one parameter built through `newFunctionParameterBuilder`. |
| `CastCollectionToFeatureConverter.java` | Converts `CastCollection` expression into a `FeatureTargetMapping`/ID attribute feature via `newIdAttributeBuilder`; uses `JoinFactory` to join the target entity and registers the feature on `context`. |
| `CastObjectToFeatureConverter.java` | Converts `CastObject` expression into an ID attribute feature via `newIdAttributeBuilder`; joins the cast target class through `JoinFactory`. |
| `CeilToFeatureConverter.java` | Converts numeric `CeilExpression` into a `Function` feature via `newFunctionBuilder`. |
| `ConcatenateToFeatureConverter.java` | Converts `Concatenate` expression into a `Function` feature; appends each operand via `newFunctionParameterBuilder`. |
| `ConstantToFeatureConverter.java` | Converts `Constant` (string/decimal/enum/instance) into a query `Constant` feature via `newConstantBuilder`. Resolves `MeasuredDecimal` measure+unit through `MeasureProvider`, scaling by unit rate at `Constants.MEASURE_RATE_CALCULATION_SCALE`. Constructor takes `Coercer`. |
| `ContainerExpressionToFeatureConverter.java` | Converts `ContainerExpression` into an ID attribute feature via `newIdAttributeBuilder`; joins to the container entity through `JoinFactory`. |
| `ContainsExpressionToFeatureConverter.java` | Converts `ContainsExpression` into a `SubSelect` feature: clones `context`, builds sub-select over the collection element `EClass` and targets `ObjectVariableReference` built with `newObjectVariableReferenceBuilder`; non-empty membership uses `getNextSubSelectAlias` and `JoinFactory`. |
| `DateAdditionExpressionToFeatureConverter.java` | Converts `DateAdditionExpression` into a `Function` feature carrying `DurationType`/`DurationUnit` measure data; validates the date operand with `checkArgument` and scales by `BigDecimal` unit factors. |
| `DateComparisonToFeatureConverter.java` | Converts `DateComparison` into a `Function` feature with comparison operand parameters via `newFunctionParameterBuilder`. |
| `DateConstructionExpressionToFeatureConverter.java` | Converts `DateConstructionExpression` into a typed `Function` feature using `FunctionSignature` plus named `ParameterName` parameters. |
| `DateDifferenceExpressionToFeatureConverter.java` | Converts `DateDifferenceExpression` into a `Function` feature; maps `DurationType`/`DurationUnit` through `AsmUtils`, validates operands with `checkArgument`, applies measure rate via `applyMeasure`. |
| `DecimalArithmeticExpressionToFeatureConverter.java` | Converts `DecimalArithmeticExpression` into a `Function` feature with left/right operand parameters. |
| `DecimalComparisonToFeatureConverter.java` | Converts `DecimalComparison` into a `Function` feature with operand parameters. |
| `DecimalOppositeToFeatureConverter.java` | Converts unary `DecimalOppositeExpression` into a `Function` feature with a single parameter. |
| `EmptyToFeatureConverter.java` | Converts `Empty` test into a `SubSelect` feature: sub-select over the collection's `EClass` with `getNextSubSelectAlias`; outer join via `JoinFactory` refutes existence. |
| `EnumerationComparisonToFeatureConverter.java` | Converts `EnumerationComparison` into a `Function` feature; reads comparator from `EnumerationComparator` operand. |
| `EnvironmentVariableToFeatureConverter.java` | Converts `EnvironmentVariable`/`StringConstant`/`MeasuredDecimalEnvironmentVariable` into a query `Variable` feature via `newVariableBuilder`; validates the expression kind with `checkArgument` and applies target unit conversion. |
| `ExistsToFeatureConverter.java` | Converts `Exists` into a `SubSelect` feature: clones `context`, sub-selects the collection element `EClass` with `getNextSubSelectAlias`, joins via `JoinFactory`. |
| `ExpressionToFeatureConverter.java` | Abstract base for all converters; exports `convert(E, Context, FeatureTargetMapping)`, `applyMeasure`, `getConstraints`. → see `ExpressionToFeatureConverter.java.AGENTS.md` |
| `ExtractDateExpressionToFeatureConverter.java` | Converts `ExtractDateExpression` into a typed `Function` feature using `FunctionSignature`/`ParameterName`. |
| `ExtractDateOfTimestampExpressionToFeatureConverter.java` | Converts `ExtractDateOfTimestampExpression` into a `Function` feature with one operand parameter. |
| `ExtractTimeExpressionToFeatureConverter.java` | Converts `ExtractTimeExpression` into a typed `Function` feature using `FunctionSignature`/`ParameterName`. |
| `ExtractTimeOfTimestampExpressionToFeatureConverter.java` | Converts `ExtractTimeOfTimestampExpression` into a `Function` feature with one operand parameter. |
| `ExtractTimestampExpressionToFeatureConverter.java` | Converts `ExtractTimestampExpression` into a typed `Function` feature using `FunctionSignature`/`ParameterName`. |
| `FloorToFeatureConverter.java` | Converts numeric `FloorExpression` into a `Function` feature via `newFunctionBuilder`. |
| `ForAllToFeatureConverter.java` | Converts `ForAll` into a `SubSelect` feature negated with `newNegationExpressionBuilder`: sub-selects the quantified `EClass` via `getNextSubSelectAlias`, builds the negated body through `EcoreUtil`. |
| `InstanceOfExpressionToFeatureConverter.java` | Converts `InstanceOfExpression` into an `INSTANCE_OF` `Function` feature plus `EntityTypeName`. Resolves `elementName` through `modelAdapter`; throws `IllegalStateException` for unknown type name or non-`EClass` type. |
| `IntegerArithmeticExpressionToFeatureConverter.java` | Converts `IntegerArithmeticExpression` into a `Function` feature with left/right operand parameters. |
| `IntegerComparisonToFeatureConverter.java` | Converts `IntegerComparison` into a `Function` feature with operand parameters. |
| `IntegerOppositeToFeatureConverter.java` | Converts unary `IntegerOppositeExpression` into a `Function` feature with a single parameter. |
| `KleeneExpressionToFeatureConverter.java` | Converts `KleeneExpression` into a `Function` feature with operand parameters. |
| `LengthToFeatureConverter.java` | Converts `Length` expression into a `Function` feature with one operand parameter. |
| `LikeToFeatureConverter.java` | Converts `Like` pattern test into a `Function` feature; materializes the pattern as `StringConstant` via `StringConstantBuilder`. |
| `LowerCaseToFeatureConverter.java` | Converts `LowerCase` expression into a `Function` feature with one operand parameter. |
| `MatchesToFeatureConverter.java` | Converts `Matches` pattern test into a `Function` feature with one operand parameter. |
| `MemberOfExpressionToFeatureConverter.java` | Converts `MemberOfExpression` into a `SubSelect` feature: sub-selects the member collection `EClass` via `getNextSubSelectAlias`, joins through `JoinFactory`. |
| `NegationExpressionToFeatureConverter.java` | Converts `NegationExpression` into a `Function` feature with one operand parameter. |
| `ObjectComparisonToFeatureConverter.java` | Converts `ObjectComparison` into a `Function` feature; compares by converted object operands. |
| `ObjectNavigationToFeatureConverter.java` | Converts `ObjectNavigationExpression` into an ID attribute feature via `newIdAttributeBuilder`; joins the resolved path with `JoinFactory`. |
| `ObjectVariableReferenceToFeatureConverter.java` | Converts `ObjectVariableReference` into an ID attribute feature via `newIdAttributeBuilder` with the referenced object's ID. |
| `PaddingToFeatureConverter.java` | Converts `PaddingExpression` into a `Function` feature; reads padding direction from source `PaddignType` enum. |
| `PositionToFeatureConverter.java` | Converts `Position` search expression into a `Function` feature with two operand parameters. |
| `ReplaceToFeatureConverter.java` | Converts `Replace` expression into a `Function` feature with operand parameters. |
| `RoundToFeatureConverter.java` | Converts `DecimalRoundExpression`/`IntegerRoundExpression` into a `Function` feature via `newFunctionBuilder`. |
| `SequenceExpressionToFeatureConverter.java` | Stub for `SequenceExpression`: `convert` always throws `UnsupportedOperationException("Not supported yet")`. |
| `StringComparisonToFeatureConverter.java` | Converts `StringComparison` into a `Function` feature with operand parameters. |
| `SubStringToFeatureConverter.java` | Converts `SubString` expression into a `Function` feature with operand parameters. |
| `SwitchExpressionToFeatureConverter.java` | Converts `SwitchExpression` into a chain of `CASE_WHEN` `Function` features, one per `SwitchCase` with `CONDITION`/`LEFT` parameters; wraps string-literal case results in substring functions via `wrapInSubstringIfStringConstant`. |
| `TimeAdditionExpressionToFeatureConverter.java` | Converts `TimeAdditionExpression` into a typed `Function` feature (`FunctionSignature`/`ParameterName`); carries `DurationType`/`DurationUnit` measure data, validated by `checkArgument`. |
| `TimeAsMillisecondsExpressionToFeatureConverter.java` | Converts `TimeAsMillisecondsExpression` into a `Function` feature with one operand parameter. |
| `TimeComparisonToFeatureConverter.java` | Converts `TimeComparison` into a typed `Function` feature using `FunctionSignature`/`ParameterName`. |
| `TimeConstructionExpressionToFeatureConverter.java` | Converts `TimeConstructionExpression` into a typed `Function` feature using `FunctionSignature`/`ParameterName`. |
| `TimeDifferenceExpressionToFeatureConverter.java` | Converts `TimeDifferenceExpression` into a typed `Function` feature; maps `DurationType`/`DurationUnit` through `AsmUtils`, validates operands with `checkArgument`. |
| `TimeFromMillisecondsExpressionToFeatureConverter.java` | Converts `TimeFromMillisecondsExpression` into a `Function` feature with one operand parameter. |
| `TimestampAdditionExpressionToFeatureConverter.java` | Converts `TimestampAdditionExpression` into a `Function` feature with `DurationType`/`DurationUnit` measure data and `BigDecimal` unit scaling; validates operands with `checkArgument`. |
| `TimestampArithmeticExpressionToFeatureConverter.java` | Converts `TimestampArithmeticExpression` into a `Function` feature over `List`-gathered component expressions. |
| `TimestampAsMillisecondsExpressionToFeatureConverter.java` | Converts `TimestampAsMillisecondsExpression` into a `Function` feature with one operand parameter. |
| `TimestampComparisonToFeatureConverter.java` | Converts `TimestampComparison` into a `Function` feature with operand parameters. |
| `TimestampConstructionExpressionToFeatureConverter.java` | Converts `TimestampConstructionExpression` into a typed `Function` feature using `FunctionSignature`/`ParameterName`. |
| `TimestampDifferenceExpressionToFeatureConverter.java` | Converts `TimestampDifferenceExpression` into a `Function` feature; maps `DurationType`/`DurationUnit` through `AsmUtils`, validates operands with `checkArgument`, applies measure rate. |
| `TimestampFromMillisecondsExpressionToFeatureConverter.java` | Converts `TimestampFromMillisecondsExpression` into a `Function` feature with one operand parameter. |
| `TrimToFeatureConverter.java` | Converts `Trim` expression into a `Function` feature; reads direction from source `TrimType` enum. |
| `TypeOfExpressionToFeatureConverter.java` | Converts `TypeOfExpression` into a `TYPE_OF` `Function` feature plus `EntityTypeName`. Resolves `elementName` through `modelAdapter`; throws `IllegalStateException` for unknown type name or non-`EClass` type. |
| `UndefinedComparisonToFeatureConverter.java` | Converts `UndefinedComparison` (undefined/null test) into a `Function` feature with one operand parameter. |
| `UpperCaseToFeatureConverter.java` | Converts `UpperCase` expression into a `Function` feature with one operand parameter. |