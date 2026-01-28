---
name: judo-runtime:query-optimization
description: Optimize JUDO query performance. Use when diagnosing slow queries, improving join strategies, understanding query caching, or tuning expression evaluation.
metadata:
  author: BlackBelt Technology
  version: "${project.version}"
---

# Query Optimization

Guide for optimizing JUDO query performance, including join strategies, caching, and expression optimization.

## Query Optimization Overview

```mermaid
flowchart TB
    subgraph "Performance Factors"
        JOIN["Join Strategy"]
        CACHE["Query Caching"]
        EXPR["Expression Complexity"]
        SUB["SubSelect Usage"]
    end
    
    subgraph "Optimization Techniques"
        EMBED["Embedded Joins"]
        STATIC["Static Navigation"]
        MEAS["Measure Conversion"]
        FILTER["Filter Placement"]
    end
    
    JOIN --> EMBED
    CACHE --> STATIC
    EXPR --> MEAS
    SUB --> FILTER
```

## Join Strategy Optimization

### Embedded vs SubSelect References

The query factory chooses between embedded JOINs and SubSelects based on these criteria:

```mermaid
flowchart TD
    REF["Reference to Process"]
    
    REF --> MANY{"Is Many?"}
    MANY -->|Yes| SUBSEL["SubSelect"]
    MANY -->|No| CIRC{"Circular?"}
    
    CIRC -->|Yes| SUBSEL
    CIRC -->|No| SAME{"Same Base?"}
    
    SAME -->|Yes| EMBED["Embedded JOIN"]
    SAME -->|No| SUBSEL
    
    EMBED --> FAST["Faster: Single Query"]
    SUBSEL --> SAFE["Safe: Avoids Cartesian"]
```

### Decision Criteria

| Criterion | Embedded JOIN | SubSelect |
|-----------|---------------|-----------|
| Single reference | Preferred | Fallback |
| Collection reference | Never | Always |
| Circular reference | Never | Always |
| Different base node | Never | Always |
| Aggregation needed | Never | Required |

### Code: Reference Handling

```java
// From QueryFactory.addReferences()
final boolean includedByJoin = !reference.isMany() &&
        !referenceChain.contains(reference) &&
        !subSelect.getNavigationJoins().stream()
            .anyMatch(j -> j instanceof SubSelectJoin && 
                           isCircularAggregation(referenceChain, reference)) &&
        AsmUtils.equals(subSelect.getBase(), context.getNode());

subSelect.setExcluding(includedByJoin);
```

## Caching Strategies

### Query Cache Structure

```mermaid
classDiagram
    class QueryFactory {
        -Map~EClass, Select~ transferObjectQueries
        -Map~EReference, SubSelect~ navigationQueries
        -Map~EAttribute, SubSelect~ dataQueries
    }
    
    note for QueryFactory "Queries are cached by:<br/>- Transfer object type<br/>- Navigation reference<br/>- Static data attribute"
```

### Cache Lookup

```java
// Get cached query for transfer object type
Optional<Select> query = queryFactory.getQuery(transferObjectType);

// Get cached navigation subselect
Optional<SubSelect> navigation = queryFactory.getNavigation(navigationReference);

// Get cached data query (for static attributes)
Optional<SubSelect> dataQuery = queryFactory.getDataQuery(attribute);
```

### Static vs Dynamic Navigations

Static navigations can be pre-computed and cached:

```mermaid
flowchart LR
    subgraph "Static Navigation"
        STATIC["No Variables<br/>Cached at Startup"]
    end
    
    subgraph "Dynamic Navigation"
        DYNAMIC["Has Variables<br/>Computed Per Request"]
    end
    
    STATIC -->|Fast| CACHED["Pre-built SubSelect"]
    DYNAMIC -->|Flexible| RUNTIME["Runtime Generation"]
```

**Check if static**:
```java
// Reference is static if no variables in expression
boolean isStatic = queryFactory.isStaticReference(reference);
boolean isStaticAttr = queryFactory.isStaticAttribute(attribute);
```

## Expression Optimization

### Measure Conversion Efficiency

Measure rate calculations use configurable precision:

```java
// From Constants.java
public static final int MEASURE_RATE_CALCULATION_SCALE = 30;  // High precision for rates
public static final int MEASURE_CONVERTING_PRECISION = 100;   // RDBMS precision
public static final int MEASURE_CONVERTING_SCALE = 15;        // RDBMS scale
```

**Optimization**: Only apply measure conversion when units differ:

```java
// From ExpressionToFeatureConverter.applyMeasureByUnit()
if (Objects.equals(dividend, divisor)) {
    return feature;  // Skip conversion - same units
} else {
    // Apply rate conversion
    final Feature rateConstant = newConstantBuilder()
            .withValue(divisor.divide(dividend, 
                MEASURE_RATE_CALCULATION_SCALE, 
                RoundingMode.HALF_UP))
            .build();
    // ... create multiplication function
}
```

### Function Constraint Propagation

Constraints (precision, scale, maxLength) are propagated to optimize storage:

```mermaid
flowchart LR
    ATTR["Source Attribute<br/>precision=10, scale=2"]
    FUNC["Function<br/>(e.g., MULTIPLY)"]
    CONST["Constraints<br/>Applied to Result"]
    
    ATTR --> FUNC
    FUNC --> CONST
```

## SubSelect Optimization

### Aggregation Placement

Place aggregations in SubSelects to avoid row multiplication:

```mermaid
flowchart TB
    subgraph "Bad: Aggregation in Main Query"
        BAD_MAIN["Main Query"]
        BAD_JOIN["JOIN orders"]
        BAD_AGG["SUM(amount)"]
        BAD_MAIN --> BAD_JOIN --> BAD_AGG
        BAD_NOTE["Problem: Rows multiplied<br/>before aggregation"]
    end
    
    subgraph "Good: SubSelect Aggregation"
        GOOD_MAIN["Main Query"]
        GOOD_SUB["SubSelect"]
        GOOD_AGG["SUM(amount)"]
        GOOD_MAIN --> GOOD_SUB --> GOOD_AGG
        GOOD_NOTE["Correct: Aggregation<br/>in isolated scope"]
    end
```

### SubSelect Types

| Type | Use Case | Performance |
|------|----------|-------------|
| Simple SubSelect | Collection references | Good |
| Correlated SubSelect | Filtered collections | Slower |
| Aggregated SubSelect | COUNT, SUM, etc. | Requires grouping |

## Filter Optimization

### Filter Placement

Filters should be placed as early as possible in the join chain:

```mermaid
flowchart LR
    subgraph "Early Filter (Good)"
        F1["Filter<br/>status='ACTIVE'"]
        J1["Join orders"]
        F1 --> J1
        NOTE1["Fewer rows to join"]
    end
    
    subgraph "Late Filter (Bad)"
        J2["Join orders"]
        F2["Filter<br/>status='ACTIVE'"]
        J2 --> F2
        NOTE2["All rows joined first"]
    end
```

### Implementing Early Filters

```java
// Filter is added to node in addFilter()
private void addFilter(MappedTransferObjectTypeBindings bindings, Context context) {
    final Node self = context.getVariables().get(JqlExpressionBuilder.SELF_NAME);
    if (bindings.getFilter() != null) {
        final Feature feature = dataExpressionToFeature(
            bindings.getFilter(), context, null, null);
        self.getFilters().add(newFilterBuilder()
                .withAlias("f" + self.getAlias())
                .withFeature(feature)
                .build());
    }
}
```

## Performance Anti-Patterns

### Anti-Pattern 1: Deep Navigation Chains

```mermaid
flowchart LR
    A["order"] --> B["customer"] --> C["address"] --> D["city"] --> E["country"]
    
    NOTE["5 JOINs - Consider denormalization"]
```

**Solution**: Use CustomJoinDefinition for complex paths:

```java
CustomJoinDefinition.builder()
    .navigationSql("SELECT country_id FROM order_country_view WHERE order_id = :sourceId")
    .sourceIdParameterName("sourceId")
    .build();
```

### Anti-Pattern 2: N+1 Queries

```mermaid
sequenceDiagram
    participant App
    participant DB
    
    App->>DB: SELECT * FROM orders
    loop For each order
        App->>DB: SELECT * FROM order_items WHERE order_id = ?
    end
    
    Note over App,DB: N+1 problem: 1 + N queries
```

**Solution**: Use SubSelect with batch loading (built into QueryFactory):

```java
// Navigation queries use IN clause batching
subSelect.setSourceIdSetParameter("sourceIds");  // For batch queries
```

### Anti-Pattern 3: Circular Aggregation

```mermaid
flowchart LR
    A["Order"] --> B["Items"]
    B --> C["Product"]
    C --> A
    
    NOTE["Circular reference detected"]
```

**Detection**: QueryFactory automatically detects and handles:

```java
private boolean isCircularAggregation(List<EClass> sourceTypes, 
                                       List<EClass> checkedTypes, 
                                       List<EReference> references) {
    if (references.stream()
            .map(r -> r.getEReferenceType())
            .anyMatch(t -> sourceTypes.contains(t))) {
        return true;  // Circular detected - use SubSelect instead
    }
    // ... recursive check
}
```

## Monitoring Query Performance

### Enable Query Logging

```java
// In logback configuration
<logger name="hu.blackbelt.judo.runtime.core.query" level="DEBUG"/>
<logger name="hu.blackbelt.judo.runtime.core.query.QueryFactory" level="TRACE"/>
```

### Key Log Messages

| Message Pattern | Meaning |
|-----------------|---------|
| `Creating logical query skeleton for...` | Query generation started |
| `Adding SUBQUERY reference: ...` | SubSelect chosen over JOIN |
| `Adding JOIN reference: ...` | Embedded JOIN used |
| `Static navigation: ...` | Navigation is cacheable |
| `Invalid query model: ...` | Query validation failed |

### Performance Metrics

```java
// Track query generation time
long start = System.currentTimeMillis();
QueryFactory factory = new QueryFactory(...);
long duration = System.currentTimeMillis() - start;
log.info("Query factory initialization: {}ms", duration);

// Track query count
log.info("Cached queries: {}", factory.transferObjectQueries.size());
log.info("Navigation queries: {}", factory.navigationQueries.size());
```

## Optimization Checklist

### Query Structure
- [ ] Single references use embedded JOINs
- [ ] Collection references use SubSelects
- [ ] Circular references avoided in JOINs
- [ ] Filters placed early in chain

### Caching
- [ ] Static navigations are pre-computed
- [ ] Query cache is populated at startup
- [ ] Reusable queries are cached

### Expressions
- [ ] Measure conversions only when needed
- [ ] Constraints propagated correctly
- [ ] Aggregations in SubSelects

### Custom Joins
- [ ] Complex navigations use CustomJoinDefinition
- [ ] Views/functions for complex paths
- [ ] Batch parameters for collections

## See Also

- `/judo-runtime:query-translation` - Query translation details
- `/judo-runtime:custom-queries` - Custom query implementation
- `agent-docs/performance.md` - Performance tuning guide
