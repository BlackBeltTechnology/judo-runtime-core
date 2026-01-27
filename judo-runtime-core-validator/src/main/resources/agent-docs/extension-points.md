# Validator Module Extension Points

## Validator Interface

**Purpose**: Implement custom validation logic

**Location**: `hu.blackbelt.judo.runtime.core.validator.Validator`

```java
public interface Validator {
    
    /**
     * Determines if this validator applies to the given feature.
     */
    boolean isApplicable(EStructuralFeature feature);
    
    /**
     * Validates a value and returns validation results.
     */
    Collection<ValidationResult> validateValue(
        Payload payload,
        EStructuralFeature feature,
        Object value,
        Map<String, Object> context
    );
    
    /**
     * Helper to add validation errors.
     */
    static void addValidationError(
        Map<String, Object> parameters,
        Object location,
        Collection<ValidationResult> validationResults,
        String code);
}
```

**Example Implementation**:

```java
public class PhoneNumberValidator implements Validator {
    
    private static final Pattern PHONE_PATTERN = 
        Pattern.compile("^\\+?[0-9]{10,14}$");
    
    @Override
    public boolean isApplicable(EStructuralFeature feature) {
        return feature instanceof EAttribute 
            && feature.getName().toLowerCase().contains("phone");
    }
    
    @Override
    public Collection<ValidationResult> validateValue(
            Payload payload, EStructuralFeature feature, 
            Object value, Map<String, Object> context) {
        
        Collection<ValidationResult> results = new ArrayList<>();
        
        if (value instanceof String && !((String) value).isEmpty()) {
            if (!PHONE_PATTERN.matcher((String) value).matches()) {
                Validator.addValidationError(
                    Map.of("value", value),
                    context.get(DefaultPayloadValidator.LOCATION_KEY),
                    results,
                    "INVALID_PHONE_NUMBER"
                );
            }
        }
        
        return results;
    }
}
```

---

## ValidatorProvider Interface

**Purpose**: Manage validator lifecycle

**Location**: `hu.blackbelt.judo.runtime.core.validator.ValidatorProvider`

```java
public interface ValidatorProvider {
    void addValidator(Validator validator);
    void removeValidator(Validator validator);
    void removeValidatorType(Class<? extends Validator> validatorType);
    void replaceValidator(Validator validator);
    <T extends Validator> Optional<Validator> getInstance(Class<T> clazz);
    Collection<Validator> getValidators();
}
```

**Registration with Guice**:

```java
public class CustomValidatorModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(PhoneNumberValidator.class).asEagerSingleton();
    }
    
    @Provides
    @Singleton
    public ValidatorProvider validatorProvider(
            PhoneNumberValidator phoneValidator,
            DefaultValidatorProvider defaultProvider) {
        defaultProvider.addValidator(phoneValidator);
        return defaultProvider;
    }
}
```

---

## DefaultPayloadValidator

**Purpose**: Main validation orchestrator

**Key Methods**:

```java
// Validate entire payload
List<ValidationResult> validatePayload(
    EClass transferObjectType,
    Payload input,
    Map<String, Object> validationContext,
    boolean throwValidationException);

// Validate single attribute
Collection<ValidationResult> validateAttribute(
    EAttribute attribute,
    Payload instance,
    Map<String, Object> validationContext);

// Validate reference
List<ValidationResult> validateReference(
    EReference reference,
    Payload instance,
    Map<String, Object> validationContext,
    boolean ignoreInvalidValues);
```

**Context Keys**:

| Key | Type | Description |
|-----|------|-------------|
| `VALIDATE_MISSING_FEATURES_KEY` | Boolean | Check required fields |
| `IGNORE_INVALID_VALUES_KEY` | Boolean | Skip invalid value checks |
| `VALIDATE_FOR_CREATE_OR_UPDATE_KEY` | Boolean | Handle derived refs |
| `NO_TRAVERSE_KEY` | Boolean | Skip nested traversal |
| `LOCATION_KEY` | String | Current validation path |

---

## Built-in Validators

### MaxLengthValidator

Validates string max length from `@constraints(maxLength=N)`.

### MinLengthValidator

Validates string min length from `@constraints(minLength=N)`.

### PatternValidator

Validates string pattern from `@constraints(pattern="regex")`.

### PrecisionValidator

Validates numeric precision/scale from `@constraints(precision=N, scale=M)`.

### RangeValidator

Validates embedded reference values are within allowed range.

### UniqueAttributeValidator

Validates identifier attribute uniqueness across payload and database.
