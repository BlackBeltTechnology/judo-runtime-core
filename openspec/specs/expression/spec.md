# JUDO Expression Specification

## Purpose
Collects and resolves expression bindings between ASM transfer object types and their underlying entity types, building expression trees for attribute getters/setters, reference getters/setters, and filters that serve as the foundation for logical query generation.

## Architecture
The module contains four classes:

- **TransferObjectTypeBindingsCollector** -- The central collector that traverses all ASM transfer object types and resolves their expression bindings from the expression model. It maintains:
  - A map of `EClass -> EntityTypeExpressions` for all entity types
  - Caches of static flags for attributes (`staticFlagOfAttributes`) and references (`staticFlagOfReferences`)
  - Uses `ExpressionEvaluator` to determine variable scopes for static/dynamic classification

- **MappedTransferObjectTypeBindings** -- Holds bindings for a mapped transfer object type: entity type, transfer object type, getter/setter attribute expressions (`Map<EAttribute, DataExpression>`), getter/setter reference expressions (`Map<EReference, ReferenceExpression>`), a filter (`LogicalExpression`), and child references (`Map<EReference, MappedTransferObjectTypeBindings>`).

- **UnmappedTransferObjectTypeBindings** -- Holds bindings for unmapped transfer object types: data expressions (`Map<EAttribute, DataExpression>`) and navigation expressions (`Map<EReference, ReferenceExpression>`).

- **EntityTypeExpressions** -- Simple holder for an entity type's getter attribute and reference expressions.

## Requirements

### Requirement: Mapped transfer object graph resolution
The `TransferObjectTypeBindingsCollector.getTransferObjectGraph()` method SHALL resolve the complete expression tree for a mapped transfer object type including all attribute and reference bindings.

#### Scenario: Resolving a mapped transfer object type
- **GIVEN** a `TransferObjectTypeBindingsCollector` initialized with valid ASM and expression resource sets, and a mapped transfer object type with attribute and reference bindings
- **WHEN** `getTransferObjectGraph(mappedTransferObjectType)` is called
- **THEN** an `Optional<MappedTransferObjectTypeBindings>` is returned containing the entity type, getter/setter attribute expressions, getter/setter reference expressions, and nested reference bindings

#### Scenario: Rejecting an unmapped or entity type
- **GIVEN** a mapped transfer object type that has no mapped entity type
- **WHEN** `getTransferObjectGraph(mappedTransferObjectType)` is called
- **THEN** `Optional.empty()` is returned

#### Scenario: Circular reference handling
- **GIVEN** two mapped transfer object types that reference each other
- **WHEN** `getTransferObjectGraph(typeA)` is called
- **THEN** the second type is resolved from the `processedMappedTransferObjectTypeBindings` cache without infinite recursion

### Requirement: Unmapped transfer object bindings resolution
The `TransferObjectTypeBindingsCollector.getTransferObjectBindings()` method SHALL resolve expression bindings for unmapped (non-mapped, non-entity) transfer object types.

#### Scenario: Resolving an unmapped transfer object type
- **GIVEN** an unmapped transfer object type with derived attributes and references that have expression bindings
- **WHEN** `getTransferObjectBindings(unmappedTransferObjectType)` is called
- **THEN** an `Optional<UnmappedTransferObjectTypeBindings>` is returned with `dataExpressions` containing `DataExpression` bindings for derived attributes and `navigationExpressions` containing `ReferenceExpression` bindings for derived references

#### Scenario: Rejecting a mapped or entity type
- **GIVEN** a mapped transfer object type or an entity type
- **WHEN** `getTransferObjectBindings(type)` is called
- **THEN** `Optional.empty()` is returned

### Requirement: Attribute binding role classification
The collector SHALL classify attribute bindings into getter and setter roles based on the `AttributeBindingRole` of the expression model.

#### Scenario: Getter attribute binding
- **GIVEN** an `AttributeBinding` with role `GETTER` and a `DataExpression`
- **WHEN** the transfer object graph is resolved
- **THEN** the binding is placed in `getterAttributeExpressions` of the `MappedTransferObjectTypeBindings`

#### Scenario: Setter attribute binding
- **GIVEN** an `AttributeBinding` with role `SETTER` and a `DataExpression`
- **WHEN** the transfer object graph is resolved
- **THEN** the binding is placed in `setterAttributeExpressions` of the `MappedTransferObjectTypeBindings`

#### Scenario: Invalid getter binding type
- **GIVEN** an `AttributeBinding` with role `GETTER` whose expression is not a `DataExpression`
- **WHEN** the transfer object graph is resolved
- **THEN** an `IllegalStateException` is thrown with message "Getter binding is not a DataExpression"

### Requirement: Reference binding role classification
The collector SHALL classify reference bindings into getter and setter roles based on the `ReferenceBindingRole` of the expression model.

#### Scenario: Getter reference binding
- **GIVEN** a `ReferenceBinding` with role `GETTER` and a `ReferenceExpression`
- **WHEN** the transfer object graph is resolved
- **THEN** the binding is placed in `getterReferenceExpressions` of the `MappedTransferObjectTypeBindings`

#### Scenario: Setter reference binding
- **GIVEN** a `ReferenceBinding` with role `SETTER` and a `ReferenceExpression`
- **WHEN** the transfer object graph is resolved
- **THEN** the binding is placed in `setterReferenceExpressions` of the `MappedTransferObjectTypeBindings`

### Requirement: Filter binding resolution
The collector SHALL resolve `FilterBinding` expressions and set them as the filter on the `MappedTransferObjectTypeBindings`.

#### Scenario: Resolving a filter binding
- **GIVEN** a `FilterBinding` for a mapped transfer object type with a `LogicalExpression`
- **WHEN** the transfer object graph is resolved
- **THEN** `MappedTransferObjectTypeBindings.getFilter()` returns the `LogicalExpression`

#### Scenario: Invalid filter expression type
- **GIVEN** a `FilterBinding` whose expression is not a `LogicalExpression`
- **WHEN** the transfer object graph is resolved
- **THEN** an `IllegalStateException` is thrown with message "Filter binding is not a LogicalExpression"

### Requirement: Static expression detection
The `TransferObjectTypeBindingsCollector` SHALL determine whether an attribute or reference expression is static (has no variables in scope).

#### Scenario: Static attribute expression
- **GIVEN** an entity attribute whose getter expression has no variables in scope
- **WHEN** `isStaticAttribute(attribute)` is called
- **THEN** `true` is returned

#### Scenario: Dynamic attribute expression
- **GIVEN** an entity attribute whose getter expression has variables in scope
- **WHEN** `isStaticAttribute(attribute)` is called
- **THEN** `false` is returned

#### Scenario: Unknown attribute
- **GIVEN** an entity attribute that has no registered expression binding
- **WHEN** `isStaticAttribute(attribute)` is called
- **THEN** `null` is returned

### Requirement: Entity type expressions map
The `TransferObjectTypeBindingsCollector` SHALL maintain an unmodifiable map of entity types to their getter expressions.

#### Scenario: Accessing entity type expressions
- **GIVEN** a fully initialized `TransferObjectTypeBindingsCollector`
- **WHEN** `getEntityTypeExpressionsMap()` is called
- **THEN** an unmodifiable `Map<EClass, EntityTypeExpressions>` is returned containing all entity types with their getter attribute and reference expressions
