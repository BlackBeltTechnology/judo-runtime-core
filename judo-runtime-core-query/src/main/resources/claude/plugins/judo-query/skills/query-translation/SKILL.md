---
name: judo-runtime:query-translation
description: Understand JUDO query model to SQL translation. Use when working with logical queries, expression-to-feature conversion, navigation joins, or debugging query generation.
metadata:
  author: BlackBelt Technology
  version: "${project.version}"
---

# Query Translation

Guide for understanding the JUDO query model translation from expressions to logical queries that can be converted to SQL.

## Query Translation Architecture

```mermaid
flowchart TB
    subgraph "Input Models"
        ASM["ASM Model<br/>(Transfer Objects)"]
        EXPR["Expression Model<br/>(Bindings)"]
        MEASURE["Measure Model<br/>(Units)"]
    end
    
    subgraph "Query Factory Layer"
        QF["QueryFactory"]
        JF["JoinFactory"]
        FF["FeatureFactory"]
        CTX["Context"]
    end
    
    subgraph "Output"
        QM["Query Model<br/>(Logical Queries)"]
        SEL["Select"]
        JOIN["Joins"]
        FEAT["Features"]
    end
    
    ASM --> QF
    EXPR --> QF
    MEASURE --> FF
    
    QF --> JF
    QF --> FF
    QF --> CTX
    
    JF --> JOIN
    FF --> FEAT
    QF --> SEL
    
    SEL --> QM
    JOIN --> QM
    FEAT --> QM
```

## Core Components

### QueryFactory

The main entry point for logical query generation. Creates queries for mapped transfer object types.

```mermaid
classDiagram
    class QueryFactory {
        -ResourceSet asmResourceSet
        -TransferObjectTypeBindingsCollector bindingsCollector
        -FeatureFactory featureFactory
        -JoinFactory joinFactory
        -Map~EClass, Select~ transferObjectQueries
        -Map~EReference, SubSelect~ navigationQueries
        +getQuery(EClass transferObjectType) Optional~Select~
        +getNavigation(EReference navigation) Optional~SubSelect~
        +dataExpressionToFeature(DataExpression, Context, Target, EAttribute) Feature
    }
    
    class JoinFactory {
        -AsmModelAdapter modelAdapter
        -FeatureFactory featureFactory
        +convertNavigationToJoins(Context, Node, ReferenceExpression, boolean) PathEnds
        -buildPath(Node, Context, Node, Deque~Navigation~) PathEndsBuilder
        -extractNavigationsFromExpression(Context, ReferenceExpression, Deque, boolean) Node
    }
    
    class FeatureFactory {
        -Map~Class, ExpressionToFeatureConverter~ converters
        +convert(Expression, Context, FeatureTargetMapping) Feature
    }
    
    QueryFactory --> JoinFactory
    QueryFactory --> FeatureFactory
    JoinFactory --> FeatureFactory
```

### Query Generation Flow

```mermaid
sequenceDiagram
    participant Client
    participant QF as QueryFactory
    participant JF as JoinFactory
    participant FF as FeatureFactory
    participant QM as Query Model
    
    Client->>QF: Constructor (asmResourceSet, expressionResourceSet, coercer)
    
    Note over QF: Phase 1: Create Main Targets
    QF->>QF: createLogicalQueryMainTarget()
    QF->>QM: Create Select with MainTarget
    
    Note over QF: Phase 2: Add Attributes & Filters
    QF->>QF: createLogicalQueryWithoutReferences()
    QF->>FF: convert(DataExpression) 
    FF->>QM: Create Feature
    
    Note over QF: Phase 3: Add References
    QF->>QF: addQueryReferences()
    QF->>JF: convertNavigationToJoins()
    JF->>QM: Create Join/SubSelect
    
    Note over QF: Phase 4: Create Navigations
    QF->>QF: createNavigations()
    QF->>QM: Create SubSelect for static navigations
    
    Client->>QF: getQuery(transferObjectType)
    QF-->>Client: Optional~Select~
```

## Query Model Structure

### Select (Main Query)

```mermaid
classDiagram
    class Select {
        +EClass from
        +String alias
        +Target mainTarget
        +List~Target~ targets
        +List~Feature~ features
        +List~Filter~ filters
        +List~Join~ joins
        +List~SubSelect~ subSelects
    }
    
    class Target {
        +EClass type
        +int index
        +Node containerWithIdFeature
        +List~ReferencedTarget~ referencedTargets
    }
    
    class Feature {
        +List~FeatureTargetMapping~ targetMappings
    }
    
    class FeatureTargetMapping {
        +Target target
        +EAttribute targetAttribute
    }
    
    Select --> Target : mainTarget
    Select --> Feature : features
    Target --> ReferencedTarget : referencedTargets
    Feature --> FeatureTargetMapping : targetMappings
```

### Join Types

```mermaid
classDiagram
    class Join {
        <<abstract>>
        +String alias
        +Node partner
        +Node base
    }
    
    class ReferencedJoin {
        +EReference reference
    }
    
    class CastJoin {
        +EClass type
    }
    
    class ContainerJoin {
        +List~EReference~ references
    }
    
    class SubSelectJoin {
        +SubSelect subSelect
    }
    
    class CustomJoin {
        +EReference transferRelation
        +String navigationSql
        +String sourceIdParameter
    }
    
    Join <|-- ReferencedJoin
    Join <|-- CastJoin
    Join <|-- ContainerJoin
    Join <|-- SubSelectJoin
    Join <|-- CustomJoin
```

## Expression to Feature Conversion

### Converter Registry

The `FeatureFactory` maintains a registry of expression-to-feature converters:

| Expression Type | Converter | SQL Function |
|-----------------|-----------|--------------|
| `Constant` | `ConstantToFeatureConverter` | Literal value |
| `AttributeSelector` | `AttributeToFeatureConverter` | Column reference |
| `IntegerArithmeticExpression` | `IntegerArithmeticExpressionToFeatureConverter` | +, -, *, / |
| `DecimalArithmeticExpression` | `DecimalArithmeticExpressionToFeatureConverter` | +, -, *, / |
| `StringComparison` | `StringComparisonToFeatureConverter` | =, <>, LIKE |
| `KleeneExpression` | `KleeneExpressionToFeatureConverter` | AND, OR |
| `NegationExpression` | `NegationExpressionToFeatureConverter` | NOT |
| `CountExpression` | `CountExpressionToFeatureConverter` | COUNT(*) |
| `IntegerAggregatedExpression` | `IntegerAggregatedExpressionToFeatureConverter` | SUM, AVG, MIN, MAX |
| `SubString` | `SubStringToFeatureConverter` | SUBSTRING |
| `Concatenate` | `ConcatenateToFeatureConverter` | CONCAT |

### Feature Types

```mermaid
classDiagram
    class Feature {
        <<abstract>>
        +List~FeatureTargetMapping~ targetMappings
    }
    
    class Constant {
        +Object value
    }
    
    class Attribute {
        +Node node
        +EAttribute sourceAttribute
    }
    
    class Function {
        +FunctionSignature signature
        +List~FunctionParameter~ parameters
        +List~FunctionConstraint~ constraints
    }
    
    class SubSelectFeature {
        +SubSelect subSelect
        +Feature feature
        +boolean aggregated
    }
    
    Feature <|-- Constant
    Feature <|-- Attribute
    Feature <|-- Function
    Feature <|-- SubSelectFeature
```

## Context Management

The `Context` class tracks query building state:

```java
@Builder
public class Context {
    // Current node being processed
    private Node node;
    
    // Created query objects to add to resource
    private List<EObject> createdQueryObjects;
    
    // Variable name -> Node mapping (e.g., "self" -> Select)
    private Map<String, Node> variables;
    
    // Counters for unique aliases
    private AtomicInteger sourceCounter;  // t01, t02, ...
    private AtomicInteger targetCounter;  // index for targets
}
```

### Variable Resolution

```mermaid
flowchart LR
    subgraph "Context Variables"
        SELF["self"]
        IT["iterator"]
        VAR["customVar"]
    end
    
    SELF --> SEL["Select (t01)"]
    IT --> JOIN["Join (j01)"]
    VAR --> FILT["Filter"]
```

## Navigation Translation

### Path Extraction

The `JoinFactory.extractNavigationsFromExpression()` method converts expressions to navigation steps:

```mermaid
flowchart TB
    EXPR["order.customer.address.city"]
    
    EXPR --> N1["Navigation: order"]
    N1 --> N2["Navigation: customer"]
    N2 --> N3["Navigation: address"]
    N3 --> N4["Navigation: city"]
    
    N4 --> JOIN1["ReferencedJoin: order"]
    JOIN1 --> JOIN2["ReferencedJoin: customer"]
    JOIN2 --> JOIN3["ReferencedJoin: address"]
    JOIN3 --> ATTR["Attribute: city"]
```

### Navigation Types

| Expression | Navigation Type | Result |
|------------|-----------------|--------|
| `ObjectNavigationExpression` | `featureName` | ReferencedJoin |
| `CollectionFilterExpression` | `condition` | Filter on Join |
| `CastObject` | `cast` | CastJoin |
| `ContainerExpression` | `container` | ContainerJoin |
| `SortExpression` | `orderBy` | OrderBy on Node |
| `SubCollectionExpression` | `limit/offset` | SubSelect with limit |
| `ObjectSelectorExpression` | `objectSelector` | SubSelectJoin |

## Custom Joins

For complex navigations, use `CustomJoinDefinition`:

```java
CustomJoinDefinition.builder()
    .navigationSql("SELECT target_id FROM complex_view WHERE source_id = :sourceId")
    .sourceIdParameterName("sourceId")
    .build();
```

Register with QueryFactory:

```java
Map<EReference, CustomJoinDefinition> customJoins = new HashMap<>();
customJoins.put(myReference, customJoinDefinition);

QueryFactory factory = QueryFactory.builder()
    .asmResourceSet(asmResourceSet)
    .measureResourceSet(measureResourceSet)
    .expressionResourceSet(expressionResourceSet)
    .coercer(coercer)
    .customJoinDefinitions(customJoins)
    .build();
```

## Debugging Query Translation

### Issue 1: Missing Query

**Symptom**: `getQuery(transferObjectType)` returns empty

**Debug**:
```java
// Check if transfer object is mapped
AsmUtils asmUtils = new AsmUtils(asmResourceSet);
boolean isMapped = asmUtils.isMappedTransferObjectType(transferObjectType);
log.debug("Is mapped: {}", isMapped);

// Check entity type mapping
Optional<EClass> entityType = asmUtils.getMappedEntityType(transferObjectType);
log.debug("Entity type: {}", entityType);
```

### Issue 2: Wrong Join Type

**Symptom**: Unexpected JOIN in generated SQL

**Debug**:
```java
Select select = queryFactory.getQuery(transferObjectType).get();
select.getJoins().forEach(join -> {
    log.debug("Join type: {}, alias: {}, partner: {}", 
        join.getClass().getSimpleName(), 
        join.getAlias(), 
        join.getPartner().getAlias());
});
```

### Issue 3: Feature Not Found

**Symptom**: `IllegalStateException: Unsupported expression`

**Check**: Verify converter exists in `FeatureFactory`:
```java
// List supported expression types
featureFactory.converters.keySet().forEach(cls -> 
    log.debug("Supported: {}", cls.getSimpleName())
);
```

## Table Alias Format

Aliases are generated using:
- Source tables: `t01`, `t02`, ... (format: `t{0,number,00}`)
- Joins: `j01`, `j02`, ...
- SubSelects: `s01`, `s02`, ...
- Filters: `f{alias}` (e.g., `ft01`)

## See Also

- `/judo-runtime:query-optimization` - Query performance optimization
- `/judo-runtime:expression-syntax` - Expression model details
- `agent-docs/architecture.md` - Query module architecture
