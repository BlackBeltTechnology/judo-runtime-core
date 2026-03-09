# DAO RDBMS Specification

## Purpose
Provides the core RDBMS-based data access layer for JUDO Runtime, translating entity model operations (CRUD, reference management, query, navigation) into SQL statements executed against relational databases through a dialect-agnostic abstraction.

## Architecture

### Key Classes and Relationships

- **`AbstractRdbmsDAO`** -- Abstract base class implementing the `DAO` interface. Delegates all concrete database operations to abstract methods while providing metrics collection via `MetricsCancelToken`/`MetricsCollector` and static feature enrichment for payload results. All public DAO methods (create, read, update, delete, reference management, navigation) route through this class.
- **`RdbmsDAOImpl`** -- Concrete implementation of `AbstractRdbmsDAO`. Wires together `SelectStatementExecutor`, `ModifyStatementExecutor`, `RdbmsBuilder`, `RdbmsResolver`, and `DataSource` to fulfill all abstract data access methods. Uses `NamedParameterJdbcTemplate` from Spring JDBC for SQL execution.
- **`Dialect`** -- Interface defining database-specific behaviors (`getName()`, `getDualTable()`). Implemented by HSQLDB and PostgreSQL modules.
- **`RdbmsInit`** -- Interface for database initialization, accepting a `DataSource` and executing setup logic (e.g., Liquibase migrations).
- **`RdbmsResolver`** -- Resolves ASM model elements (EClass, EAttribute, EReference) to their corresponding RDBMS model elements (RdbmsTable, RdbmsField, RdbmsForeignKey, RdbmsJunctionTable) using `TransformationTraceService`.
- **`RdbmsParameterMapper`** -- Interface for mapping Java attribute/reference values to SQL parameters with proper type coercion. Extended by `DefaultRdbmsParameterMapper` and dialect-specific subclasses.
- **`RdbmsBuilder`** -- Constructs SQL query result sets from the query metamodel. Coordinates `MapperFactory`, `RdbmsMapper` instances, and join processors (`AncestorJoinsProcessor`, `CastJoinProcessor`, `ContainerJoinProcessor`, `CustomJoinProcessor`, `FilterJoinProcessor`, `SimpleJoinProcessor`, `SubSelectJoinProcessor`) to translate abstract queries into `RdbmsResultSet` objects.
- **`StatementExecutor`** -- Abstract base for all statement executors, providing common constants (ID_COLUMN_NAME, ENTITY_TYPE_COLUMN_NAME, ENTITY_VERSION_COLUMN_NAME, audit columns) and reference collection utilities.
- **`ModifyStatementExecutor`** -- Orchestrates the execution order of insert, update, delete, add-reference, remove-reference, unique-check, and entity-exists-validation statements.
- **`InsertStatementExecutor`** -- Executes `InsertStatement` instances using topological sort over foreign key dependencies (via JGraphT). Two-phase insert: mandatory references first, then optional references.
- **`UpdateStatementExecutor`** -- Executes `UpdateStatement` instances, updating attributes across the inheritance chain and incrementing entity version.
- **`DeleteStatementExecutor`** -- Executes `DeleteStatement` instances using reverse topological sort. Deletes records from all tables in the entity inheritance hierarchy.
- **`SelectStatementExecutor`** -- Executes SELECT queries, mapping RDBMS result sets back to `Payload` objects. Handles pagination, ordering, filtering, embedded containments, and file-type attributes. Uses `SelectStatementExecutorQueryMetaCache` for caching query metadata.
- **`AddReferenceStatementExecutor`** -- Handles foreign key updates and junction table inserts for adding references.
- **`RemoveReferenceStatementExecutor`** -- Handles foreign key nullification and junction table deletes for removing references.
- **`CheckUniqueAttributeStatementExecutor`** -- Validates uniqueness constraints on identifier-type attributes.
- **`EntityExistsValidationStatementExecutor`** -- Validates that referenced entities exist before performing mutations.
- **`UpdateReferenceExecutor`** -- Handles reference updates on existing entities (set, unset, add, remove references).
- **`Translator`** -- Dispatches expression translation to type-specific translators (e.g., `StringConstantTranslator`, `IntegerComparisonTranslator`, `BooleanConstantTranslator`, `DateConstantTranslator`, `TimestampComparisonTranslator`, `DecimalConstantTranslator`, `LikeTranslator`, `NegationTranslator`, `KleeneTranslator`, etc.).
- **`MapperFactory`** / **`DefaultMapperFactory`** -- Factory for creating `RdbmsMapper` instances that translate query model features (Attribute, Constant, Function, SubSelect, Variable, etc.) to `RdbmsField` SQL column representations.
- **`RdbmsReference`** / **`RdbmsReferenceUtil`** -- Model and utility for resolving reference storage rules (foreign key, inverse foreign key, junction table).
- **`PayloadTraverser`** -- Utility for deep traversal of nested `Payload` structures.
- **`RdbmsInstanceCollector`** -- Collects entity instances from payloads for batch statement generation.

### Query Model (join/field classes)

- **`RdbmsJoin`** hierarchy: `RdbmsTableJoin`, `RdbmsNavigationJoin`, `RdbmsQueryJoin`, `RdbmsContainerJoin`, `RdbmsCustomJoin` -- represent different SQL JOIN types.
- **`RdbmsField`** hierarchy: `RdbmsColumn`, `RdbmsConstant`, `RdbmsFunction`, `RdbmsNamedParameter`, `RdbmsEntityTypeName` -- represent SELECT column expressions.
- **`RdbmsResultSet`** -- Aggregates fields, joins, filters, ordering, and navigation filters into a complete SQL query descriptor.
- **`RdbmsOrderBy`** -- Represents ORDER BY clause entries with descending flag.
- **`RdbmsNavigationFilter`** -- Represents navigation-based WHERE clause conditions.

## Requirements

### Requirement: CRUD Operations on Entity Types
The DAO SHALL support creating, reading, updating, and deleting entity instances identified by their `EClass` type and serializable identifier, persisting data across the full inheritance chain of tables.

#### Scenario: Create a new entity instance
- **GIVEN** a valid non-abstract `EClass` entity type and a `Payload` containing attribute values
- **WHEN** `AbstractRdbmsDAO.create(EClass, Payload, QueryCustomizer)` is called
- **THEN** `InsertStatementExecutor` SHALL insert rows into all tables in the entity's inheritance chain, with mandatory references resolved via topological sort, and return the persisted `Payload` with its assigned identifier

#### Scenario: Reject creation of abstract entity types
- **GIVEN** an abstract `EClass` entity type
- **WHEN** `InsertStatementExecutor.executeInsertStatements()` processes the insert
- **THEN** the system SHALL throw an `IllegalStateException` with a message indicating abstract types cannot be explicitly instantiated

#### Scenario: Update an existing entity instance
- **GIVEN** an existing entity identified by its identifier in the `Payload`
- **WHEN** `AbstractRdbmsDAO.update(EClass, Payload, QueryCustomizer)` is called
- **THEN** `UpdateStatementExecutor` SHALL update attribute columns across all inheritance tables and increment the `VERSION` column, returning the updated `Payload`

#### Scenario: Optimistic locking on update
- **GIVEN** an entity with a version number in the `UpdateStatement`
- **WHEN** `UpdateStatementExecutor.executeUpdateStatements()` executes the UPDATE SQL
- **THEN** the WHERE clause SHALL include `VERSION = :__version` and exactly 1 record SHALL be updated, or an `IllegalStateException` is thrown

#### Scenario: Delete an entity instance
- **GIVEN** a valid entity identifier
- **WHEN** `AbstractRdbmsDAO.delete(EClass, Serializable)` is called
- **THEN** `DeleteStatementExecutor` SHALL delete rows from all tables in the entity's type hierarchy (subtypes and supertypes), using reverse topological sort to respect foreign key constraints

#### Scenario: Delete verifies maximum one record affected
- **GIVEN** a DELETE SQL executed against a table in the inheritance chain
- **WHEN** `DeleteStatementExecutor` processes the delete
- **THEN** the system SHALL verify that at most 1 record was deleted per table, throwing `IllegalStateException` otherwise

### Requirement: Entity Retrieval and Search
The DAO SHALL support retrieving entities by identifier, retrieving all instances of a type, and searching with filter/order/pagination customization.

#### Scenario: Get entity by identifier
- **GIVEN** a valid `EClass` and a serializable identifier
- **WHEN** `AbstractRdbmsDAO.getByIdentifier(EClass, Serializable)` is called
- **THEN** `SelectStatementExecutor` SHALL execute a SELECT query filtered by the identifier and return an `Optional<Payload>` containing the entity data with static features enriched

#### Scenario: Search entities with QueryCustomizer
- **GIVEN** a valid `EClass` and a `QueryCustomizer` with filter, ordering, and pagination parameters
- **WHEN** `AbstractRdbmsDAO.search(EClass, QueryCustomizer)` is called
- **THEN** `SelectStatementExecutor` SHALL build and execute a query with the applied filters, sort order, and limit/offset, returning a list of `Payload` results with static features added

#### Scenario: Count entities
- **GIVEN** a valid `EClass` and an optional `QueryCustomizer`
- **WHEN** `AbstractRdbmsDAO.count(EClass, QueryCustomizer)` is called
- **THEN** the system SHALL return the total count of matching entities

#### Scenario: Get all entities of a type
- **GIVEN** a valid `EClass`
- **WHEN** `AbstractRdbmsDAO.getAllOf(EClass)` is called
- **THEN** the system SHALL return all instances with static features from unmapped supertype transfer objects enriched into each payload

### Requirement: Reference Management
The DAO SHALL support setting, unsetting, adding, and removing references between entity instances, handling both foreign key and junction table storage strategies.

#### Scenario: Set a reference
- **GIVEN** an `EReference`, an entity identifier, and a collection of target identifiers
- **WHEN** `AbstractRdbmsDAO.setReference(EReference, Serializable, Collection)` is called
- **THEN** `AddReferenceStatementExecutor` SHALL update the foreign key column in the owner table or insert records into the junction table

#### Scenario: Unset a single reference
- **GIVEN** a single-valued `EReference` and an entity identifier
- **WHEN** `AbstractRdbmsDAO.unsetReference(EReference, Serializable)` is called
- **THEN** the system SHALL nullify the foreign key or remove the junction table record, after verifying the reference is not many-valued

#### Scenario: Add references to a many-valued reference
- **GIVEN** a many-valued `EReference`, an entity identifier, and additional target identifiers
- **WHEN** `AbstractRdbmsDAO.addReferences(EReference, Serializable, Collection)` is called
- **THEN** the system SHALL verify the reference is many-valued and add the new reference associations via `AddReferenceStatementExecutor`

#### Scenario: Remove references from a many-valued reference
- **GIVEN** a many-valued `EReference`, an entity identifier, and target identifiers to remove
- **WHEN** `AbstractRdbmsDAO.removeReferences(EReference, Serializable, Collection)` is called
- **THEN** `RemoveReferenceStatementExecutor` SHALL nullify foreign keys or delete junction table records for the specified associations

### Requirement: Navigation and Containment Queries
The DAO SHALL support navigating from a source entity to its referenced instances, including creating, updating, and deleting instances via navigation paths.

#### Scenario: Get navigation results
- **GIVEN** a source entity identifier and an `EReference`
- **WHEN** `AbstractRdbmsDAO.getNavigationResultAt(Serializable, EReference)` is called
- **THEN** the system SHALL return all `Payload` instances reachable through the reference, with static features enriched

#### Scenario: Create an instance via navigation
- **GIVEN** a source entity identifier, an `EReference`, and a `Payload` for the new instance
- **WHEN** `AbstractRdbmsDAO.createNavigationInstanceAt(Serializable, EReference, Payload, QueryCustomizer)` is called
- **THEN** the system SHALL insert the new entity and attach it to the source via the reference

#### Scenario: Validate navigation target before update/delete
- **GIVEN** a source entity identifier, an `EReference`, and a target `Payload`
- **WHEN** `AbstractRdbmsDAO.updateNavigationInstanceAt()` or `deleteNavigationInstanceAt()` is called
- **THEN** the system SHALL verify the target payload exists in the navigation results before performing the operation, throwing `IllegalArgumentException` otherwise

### Requirement: Static Feature Enrichment
The DAO SHALL enrich query results with static features from unmapped transfer object supertypes.

#### Scenario: Add static features to payload
- **GIVEN** an entity `Payload` and an `EClass` with unmapped transfer object supertypes that have static features
- **WHEN** any read/search/getAllOf operation returns results
- **THEN** `AbstractRdbmsDAO.addStaticFeaturesToPayload()` SHALL load and merge static features from all unmapped supertype transfer objects, caching results per `EClass` to avoid redundant queries

#### Scenario: Recursive static feature enrichment on embedded references
- **GIVEN** an entity `Payload` containing embedded (containment) reference payloads
- **WHEN** static features are being enriched
- **THEN** the system SHALL recursively enrich embedded reference payloads with their own static features

### Requirement: Insert Statement Dependency Ordering
The `InsertStatementExecutor` SHALL use topological sorting to determine the correct execution order of INSERT statements based on mandatory foreign key dependencies.

#### Scenario: Insert with mandatory foreign key dependency
- **GIVEN** two `InsertStatement` instances where entity A has a mandatory foreign key to entity B
- **WHEN** `InsertStatementExecutor.executeInsertStatements()` is called
- **THEN** entity B SHALL be inserted before entity A, as determined by topological sort over the JGraphT dependency graph

#### Scenario: Two-phase insert for optional references
- **GIVEN** an `InsertStatement` with both mandatory and optional reference dependencies
- **WHEN** `InsertStatementExecutor.executeInsertStatements()` processes the statement
- **THEN** phase 1 SHALL insert with mandatory references only, and phase 2 SHALL add optional references via `AddReferenceStatementExecutor`

### Requirement: Audit Trail Metadata
The DAO SHALL track creation and modification metadata (timestamp, username, user ID, version) for entity instances.

#### Scenario: Set creation metadata on insert
- **GIVEN** an `InsertStatement` with timestamp, userId, and userName
- **WHEN** `InsertStatementExecutor` builds the INSERT SQL
- **THEN** the SQL SHALL include `CREATE_TIMESTAMP`, `CREATE_USERNAME`, `CREATE_USER_ID`, and `VERSION` columns with proper values

#### Scenario: Update modification metadata on update
- **GIVEN** an `UpdateStatement` with timestamp, userId, and userName
- **WHEN** `UpdateStatementExecutor` builds the UPDATE SQL
- **THEN** the SQL SHALL update `UPDATE_TIMESTAMP`, `UPDATE_USERNAME`, `UPDATE_USER_ID`, and increment `VERSION` by 1

### Requirement: Uniqueness Constraint Validation
The DAO SHALL validate uniqueness of identifier-type attributes before persisting changes.

#### Scenario: Check unique attribute during insert or update
- **GIVEN** a `CheckUniqueAttributeStatement` for an entity with identifier-type attributes
- **WHEN** `CheckUniqueAttributeStatementExecutor.executeUniqueAttributeStatements()` is called
- **THEN** the system SHALL query the database for existing records with the same attribute values (excluding the current entity) and raise a validation error if duplicates exist

### Requirement: Entity Existence Validation
The DAO SHALL validate that referenced entities exist before performing mutations.

#### Scenario: Validate entity exists before mutation
- **GIVEN** an `InstanceExistsValidationStatement` referencing an entity by type and identifier
- **WHEN** `EntityExistsValidationStatementExecutor.executeEntityExistsValidationStatements()` is called
- **THEN** the system SHALL query the entity table by ID and throw a `ValidationException` if the entity does not exist

### Requirement: ASM-to-RDBMS Model Resolution
The `RdbmsResolver` SHALL resolve Abstract Syntax Model (ASM) elements to their corresponding RDBMS model elements using the `TransformationTraceService`.

#### Scenario: Resolve EClass to RdbmsTable
- **GIVEN** an `EClass` entity type
- **WHEN** `RdbmsResolver.rdbmsTable(EClass)` is called
- **THEN** exactly one `RdbmsTable` SHALL be found via the transformation trace, or an `IllegalStateException` is thrown

#### Scenario: Resolve EAttribute to RdbmsField
- **GIVEN** an `EAttribute`
- **WHEN** `RdbmsResolver.rdbmsField(EAttribute)` is called
- **THEN** exactly one `RdbmsField` SHALL be found via the transformation trace, or an `IllegalStateException` is thrown

#### Scenario: Resolve EReference to junction table
- **GIVEN** a many-to-many `EReference`
- **WHEN** `RdbmsResolver.rdbmsJunctionTable(EReference)` is called
- **THEN** exactly one `RdbmsJunctionTable` SHALL be found, checking the opposite reference if needed, or an `IllegalStateException` is thrown

### Requirement: SQL Query Building
The `RdbmsBuilder` SHALL translate abstract query model elements into `RdbmsResultSet` descriptors containing SQL fields, joins, filters, and ordering.

#### Scenario: Map query features to SQL columns
- **GIVEN** a query model with attribute selectors, constants, functions, and sub-selects
- **WHEN** `RdbmsBuilder` processes the features using the registered `RdbmsMapper` instances from `MapperFactory`
- **THEN** each feature SHALL be mapped to the appropriate `RdbmsField` subclass (`RdbmsColumn`, `RdbmsConstant`, `RdbmsFunction`, etc.)

#### Scenario: Process join navigation
- **GIVEN** a query model with navigation joins, container joins, and sub-select joins
- **WHEN** `RdbmsBuilder` invokes the join processors (`SimpleJoinProcessor`, `ContainerJoinProcessor`, `SubSelectJoinProcessor`, etc.)
- **THEN** the appropriate `RdbmsJoin` objects (`RdbmsTableJoin`, `RdbmsNavigationJoin`, `RdbmsQueryJoin`, `RdbmsContainerJoin`, `RdbmsCustomJoin`) SHALL be created with correct alias and condition mappings

### Requirement: Expression Translation
The `Translator` SHALL dispatch expression model elements to type-specific translators for SQL condition generation.

#### Scenario: Translate a typed expression
- **GIVEN** an expression model element (e.g., `StringConstant`, `IntegerComparison`, `BooleanConstant`, `Like`, `Negation`, `Kleene`)
- **WHEN** `Translator.apply(Expression)` is called
- **THEN** the matching translator function SHALL be found by expression class and applied, or an `IllegalStateException` is thrown if no translator matches

### Requirement: Parameter Mapping
The `RdbmsParameterMapper` SHALL convert Java attribute values and reference identifiers to properly typed SQL parameters.

#### Scenario: Map attribute parameters
- **GIVEN** a map of `EAttribute` to Java objects
- **WHEN** `RdbmsParameterMapper.mapAttributeParameters()` is called
- **THEN** each attribute value SHALL be coerced to the correct Java type and assigned the proper SQL type code from `java.sql.Types`

#### Scenario: Map reference parameters
- **GIVEN** a map of `EReference` to `Serializable` identifiers
- **WHEN** `RdbmsParameterMapper.mapReferenceParameters()` is called
- **THEN** each identifier SHALL be coerced to the configured ID type with the correct SQL type

### Requirement: Metrics Collection
All DAO operations SHALL be wrapped with metrics collection for performance monitoring.

#### Scenario: Collect query metrics
- **GIVEN** any DAO read operation (getAllOf, search, getByIdentifier, etc.)
- **WHEN** the operation is invoked
- **THEN** a `MetricsCancelToken` SHALL be started with the `dao-query` metric name and closed upon completion

#### Scenario: Collect count metrics
- **GIVEN** any DAO count operation (countAllOf, count, countRangeOf, etc.)
- **WHEN** the operation is invoked
- **THEN** a `MetricsCancelToken` SHALL be started with the `dao-count` metric name and closed upon completion

### Requirement: Default Values
The DAO SHALL support reading and applying default attribute values for entity types.

#### Scenario: Get defaults of a type
- **GIVEN** a valid `EClass`
- **WHEN** `AbstractRdbmsDAO.getDefaultsOf(EClass)` is called
- **THEN** the system SHALL return a `Payload` containing the default values for all attributes of the type

#### Scenario: Apply defaults to an existing payload
- **GIVEN** a valid `EClass` and an existing `Payload`
- **WHEN** `AbstractRdbmsDAO.applyDefaultsOf(EClass, Payload)` is called
- **THEN** the system SHALL deep-apply default values to the payload for any missing attributes

### Requirement: Range Query Support
The DAO SHALL support querying the valid range of values for a reference, optionally marking selected items.

#### Scenario: Get range of a reference
- **GIVEN** an `EReference`, a contextual `Payload`, and a `QueryCustomizer`
- **WHEN** `AbstractRdbmsDAO.getRangeOf(EReference, Payload, QueryCustomizer, boolean, boolean)` is called
- **THEN** the system SHALL return the collection of valid target `Payload` instances, optionally marking items that are already selected
