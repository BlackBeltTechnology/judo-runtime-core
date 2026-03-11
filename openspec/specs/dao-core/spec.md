# JUDO DAO Core Specification

## Purpose
Provides the core data access object abstractions for processing payload-based CRUD operations against entity types mapped through ASM transfer objects, including statement generation for insert, update, delete, and reference manipulation, as well as instance graph collection for traversing entity relationships.

## Architecture
The module is organized into three packages:

- **processors** -- `PayloadDaoProcessor` is the base class providing common predicates, validation checks, and utility methods. Specialized subclasses generate `Statement` collections:
  - `InsertPayloadDaoProcessor` -- recursively generates `InsertStatement` and `AddReferenceStatement` for new entities
  - `UpdatePayloadDaoProcessor` -- compares original and updated payloads, generating `UpdateStatement`, `InsertStatement`, `DeleteStatement`, `AddReferenceStatement`, and `RemoveReferenceStatement` as needed
  - `DeletePayloadDaoProcessor` -- recursively traverses an `InstanceGraph` to generate `DeleteStatement` and `RemoveReferenceStatement` including cascade deletes
  - `AddReferencePayloadDaoProcessor` -- generates `AddReferenceStatement` and `InstanceExistsValidationStatement` for linking entities
  - `RemoveReferencePayloadDaoProcessor` -- generates `RemoveReferenceStatement` and `InstanceExistsValidationStatement` for unlinking entities

- **statements** -- An abstract `Statement` hierarchy rooted at `Statement` (holding an `InstanceValue`):
  - `InsertStatement` -- carries container, version, userId, username, timestamp
  - `UpdateStatement` -- carries version, userId, username, timestamp
  - `DeleteStatement` -- wraps an `InstanceValue` for deletion
  - `ReferenceStatement` (abstract) -- base for `AddReferenceStatement` and `RemoveReferenceStatement`
  - `ValidationStatement` -- base for `InstanceExistsValidationStatement` and `CheckUniqueAttributeStatement`

- **collectors** -- `InstanceCollector` interface and `InstanceGraph`/`InstanceReference` value objects for traversing entity containments, references, and back-references. `EmptyMapIntstanceCollector` provides a no-op implementation.

- **values** -- `InstanceValue` (type + identifier + attributes), `AttributeValue` (attribute + value), `ReferenceValue` (type + identifier + reference + opposites), `Metadata` (userId, username, timestamp).

## Requirements

### Requirement: Insert statement generation
The `InsertPayloadDaoProcessor.insert()` method SHALL analyze a mapped transfer object payload recursively and generate a collection of `Statement` objects representing the required database insert operations.

#### Scenario: Inserting a simple entity
- **GIVEN** an `InsertPayloadDaoProcessor` with a valid `ResourceSet`, `IdentifierProvider`, `QueryFactory`, and `InstanceCollector`
- **WHEN** `insert(mappedTransferObjectType, payload, true)` is called with a payload containing all mandatory attributes
- **THEN** a collection containing at least one `InsertStatement` is returned, with a generated identifier from the `IdentifierProvider`, version set to `1`, and attribute values mapped from transfer attributes to entity attributes

#### Scenario: Inserting with embedded containment references
- **GIVEN** a payload that includes nested sub-payloads without identifiers under containment references
- **WHEN** `insert(type, payload, true)` is called
- **THEN** `InsertStatement` objects are generated for each nested entity, and `AddReferenceStatement` objects link the contained entities to their containers

#### Scenario: Inserting with existing referenced entities
- **GIVEN** a payload that includes sub-payloads with identifiers under association references
- **WHEN** `insert(type, payload, true)` is called
- **THEN** `AddReferenceStatement` objects are generated referencing the existing entities, and `InstanceExistsValidationStatement` objects verify the referenced entities exist

#### Scenario: Rejecting existing entities in containment references
- **GIVEN** a payload with sub-payloads under a containment reference that include an identifier
- **WHEN** `insert(type, payload, true)` is called
- **THEN** an `IllegalStateException` is thrown because existing entities cannot be set as compositions

### Requirement: Update statement generation
The `UpdatePayloadDaoProcessor.update()` method SHALL compare original and updated payloads and generate the minimal set of statements needed to synchronize the database.

#### Scenario: Updating changed attributes
- **GIVEN** an original payload and an updated payload with the same identifier but different attribute values
- **WHEN** `update(type, originalPayload, updatedPayload, true)` is called
- **THEN** an `UpdateStatement` is generated containing only the changed attributes, plus an `InstanceExistsValidationStatement` for the target entity

#### Scenario: Adding a new containment during update
- **GIVEN** an original payload with a null containment reference and an updated payload with a new embedded sub-payload (no identifier)
- **WHEN** `update(type, originalPayload, updatedPayload, true)` is called
- **THEN** `InsertStatement` and `AddReferenceStatement` are generated for the new contained entity

#### Scenario: Removing a containment during update
- **GIVEN** an original payload with a containment reference and an updated payload where that reference is null
- **WHEN** `update(type, originalPayload, updatedPayload, true)` is called
- **THEN** `DeleteStatement` and `RemoveReferenceStatement` are generated for the removed entity

#### Scenario: Optimistic lock version mismatch
- **GIVEN** optimistic locking is enabled, and the update payload contains a `__version` that differs from the original
- **WHEN** `update(type, originalPayload, updatedPayload, true)` is called
- **THEN** an `IllegalArgumentException` is thrown with the message "Outdated instance to update"

### Requirement: Delete statement generation
The `DeletePayloadDaoProcessor.delete()` method SHALL recursively traverse the instance graph and generate all required delete and reference-removal statements.

#### Scenario: Deleting an entity with containments
- **GIVEN** a mapped transfer object type and a set of entity identifiers, where the entities have containment children
- **WHEN** `delete(type, ids)` is called
- **THEN** `DeleteStatement` objects are generated for the root entities and all contained entities, `RemoveReferenceStatement` objects unlink all references, and `InstanceExistsValidationStatement` objects verify all involved entities

#### Scenario: Cascade delete via reverse cascade annotation
- **GIVEN** an entity that has back-references annotated with `reverseCascadeDelete`
- **WHEN** `delete(type, ids)` is called
- **THEN** the cascade-annotated referencing entities are also recursively deleted

#### Scenario: Blocking delete with mandatory back-references
- **GIVEN** an entity that has back-references with `lowerBound > 0` not annotated with `reverseCascadeDelete`
- **WHEN** `delete(type, ids)` is called and back-referencing entities are not included in the delete set
- **THEN** an `IllegalStateException` is thrown indicating mandatory references cannot be removed

### Requirement: Add reference statement generation
The `AddReferencePayloadDaoProcessor.addReference()` method SHALL generate statements for linking entities via a reference.

#### Scenario: Adding a reference with existence check
- **GIVEN** an `EReference`, a set of target identifiers, and a parent identifier
- **WHEN** `addReference(reference, identifiers, parentIdentifier, true)` is called
- **THEN** for each identifier, an `InstanceExistsValidationStatement` and an `AddReferenceStatement` are generated

#### Scenario: Adding a reference without existence check
- **GIVEN** an `EReference`, a set of target identifiers, and a parent identifier
- **WHEN** `addReference(reference, identifiers, parentIdentifier, false)` is called
- **THEN** only `AddReferenceStatement` objects are generated (no existence validation)

### Requirement: Remove reference statement generation
The `RemoveReferencePayloadDaoProcessor.removeReference()` method SHALL generate statements for unlinking entities from a reference.

#### Scenario: Removing references with existence check
- **GIVEN** an `EReference`, a set of identifiers, and a parent identifier
- **WHEN** `removeReference(reference, identifiers, parentIdentifier, true)` is called
- **THEN** for each identifier, an `InstanceExistsValidationStatement` and a `RemoveReferenceStatement` are generated

### Requirement: Instance graph collection
The `InstanceCollector` interface SHALL provide methods to collect the complete relationship graph for one or more entity instances.

#### Scenario: Collecting a single entity graph
- **GIVEN** an `InstanceCollector` implementation and an entity type with a valid identifier
- **WHEN** `collectGraph(entityType, identifier)` is called
- **THEN** an `InstanceGraph` is returned containing the entity's containments, references, and back-references as `InstanceReference` collections

#### Scenario: Collecting multiple entity graphs
- **GIVEN** an `InstanceCollector` implementation and a collection of identifiers
- **WHEN** `collectGraph(entityType, ids)` is called
- **THEN** a `Map<Serializable, InstanceGraph>` is returned mapping each identifier to its corresponding graph

### Requirement: Statement hierarchy contracts
Every `Statement` SHALL carry a non-null `InstanceValue` containing the entity type (`EClass`) and identifier (`Serializable`).

#### Scenario: InsertStatement carries metadata
- **GIVEN** an `InsertStatement` built with type, identifier, container, version, userId, username, and timestamp
- **WHEN** the statement's getters are called
- **THEN** `getInstance().getType()` returns the entity EClass, `getInstance().getIdentifier()` returns the generated ID, `getVersion()` returns `1`, and `getContainer()` returns the optional container reference

#### Scenario: CheckUniqueAttributeStatement merges attributes
- **GIVEN** a `CheckUniqueAttributeStatement` created from a statement with attributes
- **WHEN** `mergeAttributes(anotherStatement)` is called
- **THEN** identifier attributes from the other statement replace existing ones, and non-identifier attributes are added

### Requirement: Payload validation predicates
The `PayloadDaoProcessor` SHALL provide static predicates for checking payload contents against structural features.

#### Scenario: Checking mandatory attributes
- **GIVEN** a list of `EAttribute` objects where some are required, and a payload missing those required attributes
- **WHEN** `checkMandatoryAttributes(attributes, payload)` is called
- **THEN** an `IllegalArgumentException` is thrown listing the missing mandatory attributes

#### Scenario: Checking reference types
- **GIVEN** a list of `EReference` objects and a payload where a single reference value is not a `Map`
- **WHEN** `checkReferences(references, payload)` is called
- **THEN** an `IllegalArgumentException` is thrown listing the mistyped references
