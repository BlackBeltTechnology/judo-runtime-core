# JUDO Runtime Core :: Query

Query processing and translation layer for converting abstract queries to executable form.

## Overview

This module provides the query factory and feature conversion system that transforms JUDO expressions into logical query structures. It processes ASM model expressions and creates query objects that can be translated to SQL by the DAO layer.

## Maven Coordinates

```xml
<dependency>
    <groupId>hu.blackbelt.judo.runtime</groupId>
    <artifactId>judo-runtime-core-query</artifactId>
    <version>${judo-runtime-core-version}</version>
</dependency>
```

**Packaging:** OSGi bundle

## Key Components

### QueryFactory
Central factory for creating logical queries from transfer object types.

```java
QueryFactory queryFactory = QueryFactory.builder()
    .asmResourceSet(asmResourceSet)
    .measureResourceSet(measureResourceSet)
    .expressionResourceSet(expressionResourceSet)
    .coercer(coercer)
    .customJoinDefinitions(customJoins)
    .build();

// Get query for a transfer object type
Optional<Select> query = queryFactory.getQuery(transferObjectType);

// Get navigation subselect
Optional<SubSelect> navigation = queryFactory.getNavigation(reference);
```

### FeatureFactory
Converts expressions to query features (columns, computed values).

### JoinFactory
Creates JOIN structures for navigation expressions and relationships.

### Context
Query building context containing variables, counters, and node references.

### CustomJoinDefinition
Defines custom SQL joins for specific references.

### Feature Converters
Located in `feature/` package, convert specific expression types:

| Converter | Purpose |
|-----------|---------|
| `AttributeToFeatureConverter` | Entity attribute access |
| `ConstantToFeatureConverter` | Literal values |
| `DecimalArithmeticExpressionToFeatureConverter` | Math operations |
| `StringComparisonToFeatureConverter` | String comparisons |
| `DateConstructionExpressionToFeatureConverter` | Date building |
| `TimestampArithmeticExpressionToFeatureConverter` | Timestamp math |
| `SwitchExpressionToFeatureConverter` | Conditional expressions |
| `LikeToFeatureConverter` | Pattern matching |
| `ContainsExpressionToFeatureConverter` | Collection membership |

### Aggregated Feature Converters
Located in `feature/aggregated/`:
- `CountExpressionToFeatureConverter` - COUNT aggregation
- `DecimalAggregatedExpressionToFeatureConverter` - SUM, AVG, etc.
- `IntegerAggregatedExpressionToFeatureConverter` - Integer aggregations
- `StringAggregatedExpressionToFeatureConverter` - String aggregations

## Query Structure

The module creates a logical query model:
- `Select` - Main query with targets and features
- `SubSelect` - Nested queries for relations
- `Join` - Table joins (inner, outer, cast)
- `Target` - Result projection (transfer object type)
- `Feature` - Column or computed value
- `Filter` - WHERE conditions

## Usage Example

```java
// Create query factory
QueryFactory factory = new QueryFactory(asmResourceSet, expressionResourceSet, coercer);

// Get query for mapped transfer object
EClass orderType = asmUtils.resolve("demo.Order").get();
Select orderQuery = factory.getQuery(orderType).orElseThrow();

// Access query components
Target mainTarget = orderQuery.getMainTarget();
List<Feature> features = orderQuery.getFeatures();
List<SubSelect> subSelects = orderQuery.getSubSelects();
```

## Related Modules

- `judo-runtime-core-expression` - Expression evaluation
- `judo-runtime-core-dao-rdbms` - SQL generation from queries
- `judo-meta-query` - Query metamodel
- `judo-meta-expression` - Expression metamodel
