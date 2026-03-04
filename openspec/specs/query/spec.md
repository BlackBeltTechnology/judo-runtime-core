# JUDO Query Specification

## Purpose
Translates ASM expression trees and transfer object type bindings into logical query models (Select, SubSelect, Join, Feature structures) that represent database queries at an abstract level, independent of specific SQL dialects or physical table/column names.

## Architecture
The module centers on four key classes and a large collection of expression-to-feature converters:

- **QueryFactory** -- The main entry point that builds logical queries for all mapped transfer object types. It coordinates `TransferObjectTypeBindingsCollector`, `FeatureFactory`, and `JoinFactory` to produce a `QueryModelResourceSupport` containing `Select` and `SubSelect` query objects. Provides `getQuery(EClass)`, `getNavigation(EReference)`, and `getDataQuery(EAttribute)` lookup methods.

- **Context** -- An immutable-ish builder-based context object carrying the current `Node`, `createdQueryObjects` list, a `Map<String, Node>` of variables, and atomic counters for source/target alias generation. Supports `clone()` and `clone(variableName, node)` for scope management.

- **FeatureFactory** -- A converter registry mapping expression types (e.g., `Concatenate`, `IntegerArithmeticExpression`, `DateComparison`) to `ExpressionToFeatureConverter` implementations. The `convert(Expression, Context, FeatureTargetMapping)` method dispatches to the appropriate converter.

- **JoinFactory** (abstract) -- Converts `ReferenceExpression` navigation paths into chains of `Join` objects (ReferencedJoin, CastJoin, ContainerJoin, SubSelectJoin) with support for filters, ordering, casting, limits, and offsets. Returns `PathEnds` with the terminal partner node and ordering/limit metadata.

- **CustomJoinDefinition** -- Holds custom SQL-based join definitions for specific references.

- **Constants** -- Shared constant values used across the query module.

Expression-to-feature converters cover string operations (Concatenate, SubString, Trim, UpperCase, LowerCase, Capitalize, Replace, Position, Length, Like, Matches, Padding, AsString), numeric operations (IntegerArithmetic, DecimalArithmetic, IntegerComparison, DecimalComparison, IntegerOpposite, DecimalOpposite, Round, Ceil, Floor, Absolute), date/time operations (DateAddition, DateDifference, DateComparison, DateConstruction, TimeAddition, TimeDifference, TimeComparison, TimeConstruction, TimestampAddition, TimestampDifference, TimestampComparison, TimestampConstruction, TimestampArithmetic, Extract variants, AsMilliseconds, FromMilliseconds), logical operations (Kleene, Negation, UndefinedComparison, EnumerationComparison, ObjectComparison, InstanceOf, TypeOf, ForAll, Exists, Contains, MemberOf, Empty), collection aggregations (Count, IntegerAggregated, DecimalAggregated, StringAggregated, DateAggregated, TimeAggregated, TimestampAggregated, ObjectSelector, ConcatenateCollection), and navigation (Attribute, ObjectNavigation, ObjectVariableReference, CastObject, CastCollection, ContainerExpression, Constant, EnvironmentVariable, Sequence, Switch).

## Requirements

### Requirement: Logical query creation for mapped transfer object types
The `QueryFactory` SHALL create a `Select` query for every mapped transfer object type in the ASM model, containing attribute features, filter conditions, and reference sub-selects/joins.

#### Scenario: Creating a query for a mapped transfer object type
- **GIVEN** a `QueryFactory` initialized with valid ASM, measure, and expression resource sets
- **WHEN** `getQuery(mappedTransferObjectType)` is called for a mapped transfer object type
- **THEN** an `Optional<Select>` is returned containing a `Select` with `from` set to the mapped entity type, a `mainTarget` with the transfer object type, and `Feature` objects for all getter attribute expressions

#### Scenario: Query not found for non-mapped type
- **GIVEN** a `QueryFactory`
- **WHEN** `getQuery(nonMappedType)` is called for a type that is not a mapped transfer object
- **THEN** `Optional.empty()` is returned

### Requirement: Navigation sub-select creation
The `QueryFactory` SHALL create `SubSelect` objects for static derived references (navigation properties) that can be resolved independently.

#### Scenario: Retrieving a navigation sub-select
- **GIVEN** a `QueryFactory` and a derived `EReference` on an unmapped transfer object type with a static reference expression
- **WHEN** `getNavigation(reference)` is called
- **THEN** an `Optional<SubSelect>` is returned containing the navigation joins and the base select of the referenced transfer object type

#### Scenario: Navigation not found for non-derived reference
- **GIVEN** a non-derived `EReference`
- **WHEN** `getNavigation(reference)` is called
- **THEN** `Optional.empty()` is returned

### Requirement: Static data sub-select creation
The `QueryFactory` SHALL create `SubSelect` objects for static derived attributes that can be computed without instance context.

#### Scenario: Retrieving a data query sub-select
- **GIVEN** a `QueryFactory` and a derived `EAttribute` with a static data expression
- **WHEN** `getDataQuery(attribute)` is called
- **THEN** an `Optional<SubSelect>` is returned containing a `Select` with the data expression converted to a `Feature`

### Requirement: Expression-to-feature conversion
The `FeatureFactory.convert()` method SHALL dispatch expression conversion to the appropriate `ExpressionToFeatureConverter` based on the expression's runtime type.

#### Scenario: Converting a supported expression
- **GIVEN** a `FeatureFactory` with registered converters and a `Concatenate` expression
- **WHEN** `convert(concatenateExpression, context, targetMapping)` is called
- **THEN** a `Feature` object is returned representing the concatenation operation, with the `targetMapping` added to `feature.getTargetMappings()`

#### Scenario: Converting an unsupported expression
- **GIVEN** a `FeatureFactory` and an expression type that has no registered converter
- **WHEN** `convert(unknownExpression, context, targetMapping)` is called
- **THEN** an `IllegalStateException` is thrown with message "Unsupported expression: <className>"

### Requirement: Navigation-to-join conversion
The `JoinFactory.convertNavigationToJoins()` method SHALL convert a `ReferenceExpression` into a chain of `Join` objects representing the navigation path.

#### Scenario: Simple reference navigation
- **GIVEN** a `ReferenceExpression` representing a single non-derived reference step
- **WHEN** `convertNavigationToJoins(context, container, expression, false)` is called
- **THEN** a `ReferencedJoin` is added to the container's joins, and `PathEnds.getPartner()` returns the terminal join node

#### Scenario: Navigation with cast
- **GIVEN** a `ReferenceExpression` containing a `CastObject` or `CastCollection` step
- **WHEN** `convertNavigationToJoins(context, container, expression, false)` is called
- **THEN** a `CastJoin` is added to the join chain with the target type

#### Scenario: Navigation with filter
- **GIVEN** a `ReferenceExpression` containing a `CollectionFilterExpression` with a condition
- **WHEN** `convertNavigationToJoins(context, container, expression, false)` is called
- **THEN** a `Filter` is added to the container's filters with the condition converted to a `Feature`

#### Scenario: Navigation with ordering
- **GIVEN** a `ReferenceExpression` containing a `SortExpression` with order-by items
- **WHEN** `convertNavigationToJoins(context, container, expression, false)` is called
- **THEN** `OrderBy` objects are added to the container and `PathEnds.isOrdered()` returns `true`

#### Scenario: Navigation with limit and offset
- **GIVEN** a `ReferenceExpression` containing a `SubCollectionExpression` with integer constant limit and offset
- **WHEN** `convertNavigationToJoins(context, container, expression, false)` is called
- **THEN** the limit and offset are set on the enclosing `SubSelect` or returned in `PathEnds`

#### Scenario: Derived reference rejected in navigation
- **GIVEN** a navigation path that includes a derived reference step
- **WHEN** `convertNavigationToJoins` processes the step
- **THEN** an `IllegalArgumentException` is thrown indicating derived references must be resolved by the expression builder

### Requirement: Query context management
The `Context` SHALL provide variable scoping and node management for query construction.

#### Scenario: Cloning context with a new variable
- **GIVEN** a `Context` with variables `{self: selectNode}`
- **WHEN** `clone("iterator", joinNode)` is called
- **THEN** a new `Context` is returned with variables `{self: selectNode, iterator: joinNode}` and `node` set to `joinNode`, while the original context remains unchanged

#### Scenario: Adding a feature to a Select node
- **GIVEN** a `Context` with `node` set to a `Select`
- **WHEN** `addFeature(feature)` is called
- **THEN** the feature is added to the Select's features list

#### Scenario: Adding a feature to a Join node
- **GIVEN** a `Context` with `node` set to a `Join`
- **WHEN** `addFeature(feature)` is called
- **THEN** the feature is added to the Join's base Select's features list

### Requirement: Query model validation
The `QueryFactory` SHALL validate the generated query model and reject invalid models.

#### Scenario: Valid query model
- **GIVEN** a correctly configured ASM model with valid expression bindings
- **WHEN** the `QueryFactory` is constructed
- **THEN** `queryModelResourceSupport.isValid()` returns `true` and no exception is thrown

#### Scenario: Invalid query model
- **GIVEN** an ASM model that produces inconsistent query structures
- **WHEN** the `QueryFactory` is constructed
- **THEN** an `IllegalStateException` is thrown with message "Invalid query model"

### Requirement: Ordered transfer relation tracking
The `QueryFactory` SHALL track which transfer relations produce ordered results based on the presence of sort expressions in their navigation.

#### Scenario: Checking if a relation is ordered
- **GIVEN** a transfer relation whose navigation expression includes a `SortExpression`
- **WHEN** `isOrdered(transferObjectRelation)` is called
- **THEN** `true` is returned

#### Scenario: Checking an unordered relation
- **GIVEN** a transfer relation whose navigation expression has no `SortExpression`
- **WHEN** `isOrdered(transferObjectRelation)` is called
- **THEN** `false` is returned

### Requirement: Custom join support
The `QueryFactory` SHALL support custom SQL-based join definitions for specific references via `CustomJoinDefinition`.

#### Scenario: Reference with custom join definition
- **GIVEN** a `QueryFactory` constructed with a `customJoinDefinitions` map containing a mapping for a derived reference
- **WHEN** the query is built for a transfer object type containing that reference
- **THEN** a `CustomJoin` is created with the configured `navigationSql`, `sourceIdParameterName`, and `sourceIdSetParameterName` instead of the default navigation join chain
