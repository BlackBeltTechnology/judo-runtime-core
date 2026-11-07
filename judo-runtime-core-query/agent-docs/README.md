# JUDO Runtime Core :: Query

Query processing and translation layer for converting abstract queries to executable form.

## Overview

This module is the runtime entry point of the logical query model. `QueryFactory` serves the logical queries (`Select`, `SubSelect`) that the DAO layer translates to SQL. The factory has two construction paths: it **builds** the query model from the ASM, measure and expression models (delegating to `Asm2Query` of `judo-tatami-asm2query`), or it **restores** a query model that was built at model build time and saved as `<name>-query.model`. `QueryModelLoadOrBuild` is the one place that decides between the two.

The builder itself (`Asm2Query`, `FeatureFactory`, `JoinFactory`, `Context`, `Constants`, the feature converters and `EntityTypeExpressions`) moved to `judo-tatami-base/judo-tatami-asm2query`, package `hu.blackbelt.judo.tatami.asm2query`.

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
    .customJoinDefinitions(customJoins)
    .build();

// Get query for a transfer object type
Optional<Select> query = queryFactory.getQuery(transferObjectType);

// Get navigation subselect
Optional<SubSelect> navigation = queryFactory.getNavigation(reference);
```

Restore a persisted query model instead of building it:

```java
QueryFactory queryFactory = QueryModelLoadOrBuild.queryModelLoadOrBuild()
    .asmModel(asmModel).asmDigest(asmDigest)
    .measureModel(measureModel).measureDigest(measureDigest)
    .expressionModel(expressionModel).expressionDigest(expressionDigest)
    .persistedQueryModel(locator)            // PersistedQueryModel, null when there is no query file
    .customJoinDefinitions(customJoins)      // any custom join selects the build path
    .mode("auto")                            // auto (default) | load | build
    .buildQueryFactory(() -> QueryFactory.builder()/* ... */.build())
    .get();
```

Modes: `auto` restores a valid artifact and otherwise builds (WARN with the reason, except when the artifact is absent); `load` fails with the reason instead of building; `build` always builds and never opens the artifact. An artifact is valid only when its format version, generator identity (`judo-tatami-asm2query` version, plus build timestamp for a SNAPSHOT), model name and the SHA-256 digests of the ASM, measure and expression files match the runtime's. The model build and the runtime therefore have to use the same `judo-tatami-asm2query` build; `load` is brittle by design.

The `coercer` argument of `QueryFactory` is deprecated and ignored.

### CustomJoinDefinition
Defines custom SQL joins for specific references. Joins are read while the query model is built; they are never persisted, so configuring any makes `auto` build and `load` fail.

### Moved to `judo-tatami-asm2query`
`FeatureFactory` (expression to feature conversion, with the converters of `feature/` and `feature/aggregated/`), `JoinFactory` (navigation to joins), `Context` (query building state), `Constants` and `EntityTypeExpressions` are in package `hu.blackbelt.judo.tatami.asm2query`. **Breaking:** `QueryFactory.dataExpressionToFeature(..., Context, ...)` and `getEntityTypeExpressionsMap()` use the new types.

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
QueryFactory factory = QueryFactory.builder()
    .asmResourceSet(asmResourceSet)
    .measureResourceSet(measureResourceSet)
    .expressionResourceSet(expressionResourceSet)
    .build();

// Get query for mapped transfer object
EClass orderType = asmUtils.resolve("demo.Order").get();
Select orderQuery = factory.getQuery(orderType).orElseThrow();

// Access query components
Target mainTarget = orderQuery.getMainTarget();
List<Feature> features = orderQuery.getFeatures();
List<SubSelect> subSelects = orderQuery.getSubSelects();
```

## Related Modules

- `judo-tatami-asm2query` - Query model builder, persistence (`QueryModelPersistence`) and expression binding collector
- `judo-runtime-core-dao-rdbms` - SQL generation from queries
- `judo-meta-query` - Query metamodel
- `judo-meta-expression` - Expression metamodel
