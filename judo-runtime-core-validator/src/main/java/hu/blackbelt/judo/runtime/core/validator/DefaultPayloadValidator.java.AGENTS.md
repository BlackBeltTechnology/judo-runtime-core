# `DefaultPayloadValidator.java`

Implements `hu.blackbelt.judo.dao.api.PayloadValidator` — the top-level entry
point that validates a payload against an ASM transfer-object `EClass` by
walking it with `PayloadTraverser` and delegating per-feature checks to every
validator registered in the injected `ValidatorProvider`.

Exports:

- `validatePayload(EClass transferObjectType, Payload input, Map<String, Object> validationContext, boolean throwValidationException)` — traverses the payload, accumulates `ValidationResult`s; throws `ValidationException` when `throwValidationException` and results are non-empty, otherwise stores them under `VALIDATION_RESULT_KEY`.
- `validateReference(EReference, Payload, Map, boolean ignoreInvalidValues)` — checks lower/upper bounds (errors `ERROR_TOO_FEW_ITEMS`/`ERROR_TOO_MANY_ITEMS`), null items (`ERROR_NULL_ITEM_IS_NOT_SUPPORTED`), non-collection where a collection is expected (`ERROR_INVALID_CONTENT`), requiredness (`ERROR_MISSING_REQUIRED_RELATION` / `ERROR_MISSING_REQUIRED_RELATION_ON_ENTITY`), and runs applicable `Validator`s.
- `validateAttribute(EAttribute, Payload, Map)` — checks requiredness (`ERROR_MISSING_REQUIRED_ATTRIBUTE` / `ERROR_MISSING_REQUIRED_ATTRIBUTE_ON_ENTITY`), empty-string rejection when constructed with `RequiredStringValidatorOption.ACCEPT_NON_EMPTY`, and runs applicable `Validator`s.
- Constants read from `validationContext`: `GLOBAL_VALIDATION_CONTEXT`, `LOCATION_KEY`, `CREATE_REFERENCE_KEY`, `NO_TRAVERSE_KEY`, `VALIDATE_FOR_CREATE_OR_UPDATE_KEY`, `VALIDATE_MISSING_FEATURES_KEY`, `VALIDATE_ROOT_MISSING_FEATURES_KEY`, `IGNORE_INVALID_VALUES_KEY`, `IS_ROOT_KEY`, `VALIDATION_RESULT_KEY`, plus payload keys `REFERENCE_ID_KEY`, `VERSION_KEY`, `SIGNED_IDENTIFIER_KEY`.
- Static model-type mappers `ATTRIBUTE_TO_MODEL_TYPE`, `REFERENCE_TO_MODEL_TYPE`.
- Enum `RequiredStringValidatorOption` — `ACCEPT_EMPTY` | `ACCEPT_NON_EMPTY`.
- Lombok `@Builder` ctor: `@NonNull AsmModel`, `@NonNull Coercer`, plus `IdentifierProvider`, `ValidatorProvider`, `RequiredStringValidatorOption`.

Contracts a caller can violate:

- Traversed reference values must be `Payload` instances; anything else throws `IllegalStateException`.
- Missing-feature validation is skipped when the instance already carries an identifier (per `IdentifierProvider`) — supply one to bypass requiredness checks.
- No validators are consulted when the injected `ValidatorProvider` registry is empty.