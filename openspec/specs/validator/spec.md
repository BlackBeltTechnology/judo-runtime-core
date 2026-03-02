# JUDO Validator Specification

## Purpose
Provides a pluggable data validation framework for enforcing business rules and constraints on payload data, including attribute-level validators (max length, min length, precision, pattern), reference-level validators (range, cardinality), and unique attribute enforcement, all orchestrated through a recursive payload traversal mechanism.

## Architecture
The module is built around three core abstractions:

- **Validator** interface -- Defines `isApplicable(EStructuralFeature)` and `validateValue(Payload, EStructuralFeature, Object, Map)` methods. Provides error code constants (e.g., `ERROR_MAX_LENGTH_VALIDATION_FAILED`) and a static `addValidationError` helper.
- **ValidatorProvider** interface -- Manages a collection of `Validator` instances with `addValidator`, `removeValidator`, `removeValidatorType`, `replaceValidator`, and `getInstance` methods. `DefaultValidatorProvider` registers `MaxLengthValidator`, `MinLengthValidator`, `PrecisionValidator`, `PatternValidator` by default, plus `RangeValidator` and `UniqueAttributeValidator` when DAO and context are available.
- **DefaultPayloadValidator** -- Implements `PayloadValidator` and uses `PayloadTraverser` to recursively walk the payload tree, invoking all applicable validators at each attribute and reference. Supports configurable validation modes via context keys (`VALIDATE_FOR_CREATE_OR_UPDATE_KEY`, `NO_TRAVERSE_KEY`, `VALIDATE_MISSING_FEATURES_KEY`, `IGNORE_INVALID_VALUES_KEY`).

Concrete validators: `MaxLengthValidator`, `MinLengthValidator`, `PrecisionValidator`, `PatternValidator`, `RangeValidator`, `UniqueAttributeValidator`, `DummyValidator`.

## Requirements

### Requirement: Validator applicability check
Each `Validator` implementation SHALL determine its applicability based on the structural feature's annotation constraints.

#### Scenario: MaxLengthValidator is applicable
- **GIVEN** an `EAttribute` with a `constraints` annotation containing a `maxLength` detail
- **WHEN** `MaxLengthValidator.isApplicable(feature)` is called
- **THEN** `true` is returned

#### Scenario: MaxLengthValidator is not applicable
- **GIVEN** an `EAttribute` without a `maxLength` constraint
- **WHEN** `MaxLengthValidator.isApplicable(feature)` is called
- **THEN** `false` is returned

#### Scenario: PrecisionValidator is applicable
- **GIVEN** an `EAttribute` with a `constraints` annotation containing a `precision` detail
- **WHEN** `PrecisionValidator.isApplicable(feature)` is called
- **THEN** `true` is returned

#### Scenario: RangeValidator is applicable
- **GIVEN** an `EReference` that is embedded and has a `range` annotation
- **WHEN** `RangeValidator.isApplicable(feature)` is called
- **THEN** `true` is returned

#### Scenario: UniqueAttributeValidator is applicable
- **GIVEN** an `EAttribute` with a mapped entity attribute marked as an identifier
- **WHEN** `UniqueAttributeValidator.isApplicable(feature)` is called
- **THEN** `true` is returned

### Requirement: Max length validation
The `MaxLengthValidator` SHALL reject String values exceeding the `maxLength` constraint.

#### Scenario: String within max length
- **GIVEN** an `EAttribute` with `maxLength=10` and a String value of length 5
- **WHEN** `validateValue(instance, feature, value, context)` is called
- **THEN** an empty collection of `ValidationResult` is returned

#### Scenario: String exceeding max length
- **GIVEN** an `EAttribute` with `maxLength=10` and a String value of length 15
- **WHEN** `validateValue(instance, feature, value, context)` is called
- **THEN** a collection containing a `ValidationResult` with code `ERROR_MAX_LENGTH_VALIDATION_FAILED` is returned

#### Scenario: Non-string value for max length
- **GIVEN** an `EAttribute` with a `maxLength` constraint and a non-String value
- **WHEN** `validateValue(instance, feature, value, context)` is called
- **THEN** an `IllegalStateException` is thrown with message "MaxLength constraint is supported on String type only"

### Requirement: Min length validation
The `MinLengthValidator` SHALL reject String values shorter than the `minLength` constraint.

#### Scenario: String below min length
- **GIVEN** an `EAttribute` with `minLength=3` and a String value of length 1
- **WHEN** `validateValue(instance, feature, value, context)` is called
- **THEN** a collection containing a `ValidationResult` with code `ERROR_MIN_LENGTH_VALIDATION_FAILED` is returned

### Requirement: Precision and scale validation
The `PrecisionValidator` SHALL reject numeric values whose precision or scale exceeds the defined constraints.

#### Scenario: Integer exceeding precision
- **GIVEN** an `EAttribute` with `precision=5` and a `BigInteger` value of `123456` (precision 6)
- **WHEN** `validateValue(instance, feature, value, context)` is called
- **THEN** a collection containing a `ValidationResult` with code `ERROR_PRECISION_VALIDATION_FAILED` is returned

#### Scenario: Decimal exceeding scale
- **GIVEN** an `EAttribute` with `precision=10` and `scale=2` and a `BigDecimal` value of `1.234` (scale 3)
- **WHEN** `validateValue(instance, feature, value, context)` is called
- **THEN** a collection containing a `ValidationResult` with code `ERROR_SCALE_VALIDATION_FAILED` is returned

#### Scenario: Non-numeric value for precision
- **GIVEN** an `EAttribute` with a `precision` constraint and a non-Number value
- **WHEN** `validateValue(instance, feature, value, context)` is called
- **THEN** an `IllegalStateException` is thrown with message "Precision/scale constraints are supported on Number types only"

### Requirement: Pattern validation
The `PatternValidator` SHALL reject String values that do not match the defined regex pattern.

#### Scenario: String not matching pattern
- **GIVEN** an `EAttribute` with `pattern="^[A-Z]+$"` and a value `"abc123"`
- **WHEN** `validateValue(instance, feature, value, context)` is called
- **THEN** a collection containing a `ValidationResult` with code `ERROR_PATTERN_VALIDATION_FAILED` is returned

#### Scenario: String matching pattern
- **GIVEN** an `EAttribute` with `pattern="^[A-Z]+$"` and a value `"ABC"`
- **WHEN** `validateValue(instance, feature, value, context)` is called
- **THEN** an empty collection of `ValidationResult` is returned

### Requirement: Range validation
The `RangeValidator` SHALL verify that a reference value's identifier exists within the computed range of the reference.

#### Scenario: Value not in range
- **GIVEN** an embedded `EReference` with a `range` annotation, and a payload value whose identifier is not found in the DAO's range query results
- **WHEN** `validateValue(instance, feature, value, context)` is called
- **THEN** a collection containing a `ValidationResult` with code `ERROR_NOT_ACCEPTED_BY_RANGE` is returned

### Requirement: Unique attribute validation
The `UniqueAttributeValidator` SHALL verify that identifier-marked attributes do not have duplicate values in the database.

#### Scenario: Duplicate identifier value on insert
- **GIVEN** an `EAttribute` mapped to an entity identifier, a payload without an existing ID (insert scenario), and the DAO returns an existing record with the same attribute value
- **WHEN** `validateValue(instance, feature, value, context)` is called
- **THEN** a collection containing a `ValidationResult` with code `ERROR_IDENTIFIER_ATTRIBUTE_UNIQUENESS_VIOLATION` is returned

#### Scenario: Duplicate within same batch insert
- **GIVEN** two payloads in the same validation context with the same identifier attribute value being inserted
- **WHEN** the second payload's value is validated
- **THEN** a `ValidationResult` with code `ERROR_IDENTIFIER_ATTRIBUTE_UNIQUENESS_VIOLATION` is returned

### Requirement: Payload validation orchestration
The `DefaultPayloadValidator.validatePayload()` method SHALL traverse the payload tree and collect all validation results from all applicable validators.

#### Scenario: Validating with missing required attribute
- **GIVEN** a transfer object type with a required attribute and a payload where that attribute is null
- **WHEN** `validatePayload(type, payload, context, true)` is called
- **THEN** a `ValidationException` is thrown containing a `ValidationResult` with code `ERROR_MISSING_REQUIRED_ATTRIBUTE`

#### Scenario: Validating a reference with too few items
- **GIVEN** a transfer object type with a many-reference having `lowerBound=2` and a payload with only 1 item
- **WHEN** `validatePayload(type, payload, context, false)` is called
- **THEN** the returned list contains a `ValidationResult` with code `ERROR_TOO_FEW_ITEMS`

#### Scenario: Validating a reference with too many items
- **GIVEN** a transfer object type with a many-reference having `upperBound=3` and a payload with 5 items
- **WHEN** `validatePayload(type, payload, context, false)` is called
- **THEN** the returned list contains a `ValidationResult` with code `ERROR_TOO_MANY_ITEMS`

#### Scenario: Validating missing required relation
- **GIVEN** a transfer object type with a required reference and a payload where that reference is null
- **WHEN** `validatePayload(type, payload, context, true)` is called
- **THEN** a `ValidationException` is thrown containing a `ValidationResult` with code `ERROR_MISSING_REQUIRED_RELATION`

### Requirement: Validator provider management
The `ValidatorProvider` SHALL support dynamic registration, removal, and replacement of `Validator` instances.

#### Scenario: Adding a custom validator
- **GIVEN** a `DefaultValidatorProvider` with default validators
- **WHEN** `addValidator(customValidator)` is called
- **THEN** `getValidators()` includes the custom validator

#### Scenario: Replacing a validator by type
- **GIVEN** a `DefaultValidatorProvider` with a `MaxLengthValidator`
- **WHEN** `replaceValidator(newMaxLengthValidator)` is called
- **THEN** the old `MaxLengthValidator` is removed and the new one is present in `getValidators()`

#### Scenario: Removing validators by type
- **GIVEN** a `DefaultValidatorProvider` with multiple validators
- **WHEN** `removeValidatorType(MaxLengthValidator.class)` is called
- **THEN** all instances of `MaxLengthValidator` are removed from `getValidators()`
