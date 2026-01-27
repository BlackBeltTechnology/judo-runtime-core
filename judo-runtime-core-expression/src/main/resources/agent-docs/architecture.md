# Expression Module Architecture

## Data Flow

```mermaid
flowchart TB
    subgraph "Input"
        EM["Expression Metamodel<br/>(EObjects)"]
        ASM["ASM Model"]
    end
    
    subgraph "Processing"
        TB["TransferObjectTypeBindingsCollector"]
        
        subgraph "Output Structures"
            MT["MappedTransferObjectTypeBindings"]
            UT["UnmappedTransferObjectTypeBindings"]
            ET["EntityTypeExpressions"]
        end
    end
    
    subgraph "Consumers"
        QF["QueryFactory"]
        AT["AttributeSelectorTranslator"]
        SQL["SQL Generation"]
    end
    
    EM --> TB
    ASM --> TB
    TB --> MT
    TB --> UT
    TB --> ET
    MT --> QF
    ET --> AT
    QF --> SQL
    AT --> SQL
```

## Class Structure

```mermaid
classDiagram
    class TransferObjectTypeBindingsCollector {
        -expressionResourceSet: ResourceSet
        -asmModel: AsmModel
        +getTransferObjectGraph(EClass): MappedTransferObjectTypeBindings
        +getTransferObjectBindings(EClass): UnmappedTransferObjectTypeBindings
        +getExpressionElement(Class~T~): Stream~T~
    }
    
    class MappedTransferObjectTypeBindings {
        -entityType: EClass
        -getterAttributeExpressions: Map
        -setterAttributeExpressions: Map
        -getterReferenceExpressions: Map
        -references: Map
        -filter: LogicalExpression
    }
    
    class EntityTypeExpressions {
        -getterExpressions: Map
        -referenceExpressions: Map
    }
    
    class UnmappedTransferObjectTypeBindings {
        -dataExpressions: Map
        -navigationExpressions: Map
    }
    
    TransferObjectTypeBindingsCollector --> MappedTransferObjectTypeBindings
    TransferObjectTypeBindingsCollector --> UnmappedTransferObjectTypeBindings
    TransferObjectTypeBindingsCollector --> EntityTypeExpressions
    MappedTransferObjectTypeBindings --> MappedTransferObjectTypeBindings : references
```

## Expression Resolution Flow

```mermaid
sequenceDiagram
    participant QF as QueryFactory
    participant TB as BindingsCollector
    participant MT as MappedBindings
    participant ET as EntityExpressions
    participant SQL as SQL Builder
    
    QF->>TB: getTransferObjectGraph(type)
    TB->>TB: Check cache
    alt Cache miss
        TB->>TB: Build expression tree
        TB->>MT: Create bindings
    end
    TB-->>QF: MappedTransferObjectTypeBindings
    
    QF->>MT: getGetterAttributeExpressions()
    MT-->>QF: Map<EAttribute, DataExpression>
    
    loop For each derived attribute
        QF->>SQL: Inline expression into query
    end
```

## Binding Types

```mermaid
flowchart LR
    subgraph "Transfer Object"
        TA["derivedPrice: Decimal"]
        TR["customer: CustomerDTO"]
    end
    
    subgraph "Binding Layer"
        GB["Getter Binding"]
        SB["Setter Binding"]
    end
    
    subgraph "Entity"
        EA["basePrice * (1 + taxRate)"]
        ER["order.customer"]
    end
    
    TA -->|GETTER| GB
    GB --> EA
    TR -->|GETTER| GB
    GB --> ER
    
    TA -->|SETTER| SB
    SB --> EA
```

## Threading Model

All internal maps use `ConcurrentHashMap`:

```mermaid
flowchart TB
    subgraph "Thread Safety"
        T1["Thread 1"] --> C["ConcurrentHashMap Cache"]
        T2["Thread 2"] --> C
        T3["Thread 3"] --> C
    end
    
    C --> MT["MappedTransferObjectTypeBindings"]
    C --> ET["EntityTypeExpressions"]
```

## Expression Evaluation

```mermaid
flowchart LR
    E["Expression"] --> EV["ExpressionEvaluator"]
    EV --> VS["getVariablesOfScope()"]
    VS --> SF{"Static?"}
    SF -->|Yes| CE["Compile-time eval"]
    SF -->|No| RE["Runtime eval"]
```
