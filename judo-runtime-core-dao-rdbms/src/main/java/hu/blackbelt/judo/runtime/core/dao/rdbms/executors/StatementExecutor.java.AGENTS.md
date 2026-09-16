# `StatementExecutor.java`

Abstract base holding the shared collaborators (`asmModel`, `rdbmsModel`, `transformationTraceService`, `rdbmsParameterMapper`, `rdbmsResolver`, `coercer`, `identifierProvider`) and creating the `RdbmsReferenceUtil`.
Declares the public column/payload key constants (`ID_COLUMN_NAME`, `ENTITY_TYPE_COLUMN_NAME`/`__entityType`, `ENTITY_VERSION_*`/`__version`, create/update username, user-id and timestamp pairs).
`protected collectRdbmsReferencesReferenceStatements` emits one `RdbmsReference` per direction (and per `EOpposite`); `collectReferenceIdentifiersForGivenIdentifier` keeps only FK/inverse-FK rules filtered by mandatory/optional.
A null `identifierProvider` defaults to `SerializableIdentifierProvider`.