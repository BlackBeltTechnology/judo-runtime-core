# AGENTS.md — `judo-runtime-core-dao-rdbms/src/main/java/hu/blackbelt/judo/runtime/core/dao/rdbms/query/translators`

Per-node `Function` translators that rewrite one `hu.blackbelt.judo.meta.expression` tree
node so transfer-object grounded select expressions become entity grounded before SQL
generation. `SelectStatementExecutor` registers every translator into a shared `Translator`
dispatcher; composite translators recurse into operands through that same dispatcher.

| File | Purpose |
| --- | --- |
| `AttributeSelectorTranslator.java` | Rewrites `AttributeSelector` from transfer-object to entity grounding, substituting mapped entity attributes and derived getter expressions. → see `AttributeSelectorTranslator.java.AGENTS.md` |
| `BooleanConstantTranslator.java` | Deep-copies `BooleanConstant` via `EcoreUtil.copy` in `apply(BooleanConstant)`. Returned node detached from source tree; caller attaches it into the rewritten expression. |
| `CustomDataTranslator.java` | Deep-copies `CustomData` via `EcoreUtil.copy` in `apply(CustomData)`. Returned node detached from source tree; caller attaches it into the rewritten expression. |
| `DateComparisonTranslator.java` | Rebuilds `DateComparison` via `newDateComparisonBuilder()`, translating left/right `DateExpression` operands with injected `translator`, preserving operator. Builder requires `@NonNull translator`. |
| `DateConstantTranslator.java` | Deep-copies `DateConstant` via `EcoreUtil.copy` in `apply(DateConstant)`. Returned node detached from source tree; caller attaches it into the rewritten expression. |
| `DecimalComparisonTranslator.java` | Rebuilds `DecimalComparison` via `newDecimalComparisonBuilder()`, translating `NumericExpression` operands with injected `translator`, preserving operator. Builder requires `@NonNull translator`. |
| `DecimalConstantTranslator.java` | Deep-copies `DecimalConstant` via `EcoreUtil.copy` in `apply(DecimalConstant)`. Returned node detached from source tree; caller attaches it into the rewritten expression. |
| `DecimalOppositeTranslator.java` | Rebuilds `DecimalOppositeExpression` via `DecimalOppositeExpressionBuilder.create()`, translating the inner `DecimalExpression` with injected `translator`. Builder requires `@NonNull translator`. |
| `EnumerationComparisonTranslator.java` | Rebuilds `EnumerationComparison` via `newEnumerationComparisonBuilder()`, translating `EnumerationExpression` operands with injected `translator`, preserving operator. Builder requires `@NonNull translator`. |
| `InstanceTranslator.java` | Rebuilds `Instance` from transfer-object to entity grounding: resolves the TO type via `AsmUtils.getClassByFQName(namespace.replace("::", ".") + "." + name)` then `getMappedEntityType`; missing type throws `IllegalStateException`. Output pinned to name `self` with entity type name plus `AsmUtils.getPackageFQName` namespace. Builder requires `@NonNull asmUtils`. |
| `IntegerComparisonTranslator.java` | Rebuilds `IntegerComparison` via `newIntegerComparisonBuilder()`, translating `IntegerExpression` operands with injected `translator`, preserving operator. Builder requires `@NonNull translator`. |
| `IntegerConstantTranslator.java` | Deep-copies `IntegerConstant` via `EcoreUtil.copy` in `apply(IntegerConstant)`. Returned node detached from source tree; caller attaches it into the rewritten expression. |
| `IntegerOppositeTranslator.java` | Rebuilds `IntegerOppositeExpression` via `IntegerOppositeExpressionBuilder.create()`, translating the inner `IntegerExpression` with injected `translator`. Builder requires `@NonNull translator`. |
| `KleeneTranslator.java` | Rebuilds `KleeneExpression` via `newKleeneExpressionBuilder()`, translating left/right `LogicalExpression` operands with injected `translator`, preserving operator. Builder requires `@NonNull translator`. |
| `LikeTranslator.java` | Rebuilds `Like` via `newLikeBuilder()`, translating the `StringExpression` operand and `StringConstant` pattern, preserving `isCaseInsensitive`. Builder requires `@NonNull translator`. |
| `LiteralTranslator.java` | Deep-copies `Literal` via `EcoreUtil.copy` in `apply(Literal)`. Returned node detached from source tree; caller attaches it into the rewritten expression. |
| `MatchesTranslator.java` | Rebuilds `Matches` via `newMatchesBuilder()`, translating `StringExpression` operand and pattern. Builder requires `@NonNull translator`. |
| `MeasuredDecimalConstantTranslator.java` | Deep-copies `MeasuredDecimal` via `EcoreUtil.copy` in `apply(MeasuredDecimal)`. Returned node detached from source tree; caller attaches it into the rewritten expression. |
| `NegationTranslator.java` | Rebuilds `NegationExpression` via `newNegationExpressionBuilder()`, translating the inner `LogicalExpression` with injected `translator`. Builder requires `@NonNull translator`. |
| `ObjectVariableReferenceTranslator.java` | Rebuilds `ObjectVariableReference` via `newObjectVariableReferenceBuilder()`, translating the variable with injected `translator`. `checkArgument` enforces variable is an `Expression`; anything else throws `IllegalArgumentException`. |
| `StringComparisonTranslator.java` | Rebuilds `StringComparison` via `newStringComparisonBuilder()`, translating `StringExpression` operands with injected `translator`, preserving operator. Builder requires `@NonNull translator`. |
| `StringConstantTranslator.java` | Deep-copies `StringConstant` via `EcoreUtil.copy` in `apply(StringConstant)`. Returned node detached from source tree; caller attaches it into the rewritten expression. |
| `TimeComparisonTranslator.java` | Rebuilds `TimeComparison` via `newTimeComparisonBuilder()`, translating `TimeExpression` operands with injected `translator`, preserving operator. Builder requires `@NonNull translator`. |
| `TimeConstantTranslator.java` | Deep-copies `TimeConstant` via `EcoreUtil.copy` in `apply(TimeConstant)`. Returned node detached from source tree; caller attaches it into the rewritten expression. |
| `TimestampComparisonTranslator.java` | Rebuilds `TimestampComparison` via `newTimestampComparisonBuilder()`, translating `TimestampExpression` operands with injected `translator`, preserving operator. Builder requires `@NonNull translator`. |
| `TimestampConstantTranslator.java` | Deep-copies `TimestampConstant` via `EcoreUtil.copy` in `apply(TimestampConstant)`. Returned node detached from source tree; caller attaches it into the rewritten expression. |
| `Translator.java` | Dispatch table for the expression rewrite. `@Getter translators` holds `LinkedHashMap<Class, Function>` keyed by expression class; `apply(Expression)` picks the first key assignable from the runtime class (insertion order) and throws `IllegalStateException` when none matches. Caller registers a translator for every reachable node type before applying. |
| `UndefinedComparisonTranslator.java` | Rebuilds `UndefinedComparison` via `newUndefinedComparisonBuilder()`, translating the inner expression with injected `translator`. Builder requires `@NonNull translator`. |