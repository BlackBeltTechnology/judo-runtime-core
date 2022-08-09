package hu.blackbelt.judo.runtime.core.validator;

import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dao.api.ValidationResult;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EStructuralFeature;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static hu.blackbelt.judo.runtime.core.validator.Validator.addValidationError;

public class PrecisionValidator implements Validator {

    private static final String PRECISION_CONSTRAINT_NAME = "precision";
    private static final String SCALE_CONSTRAINT_NAME = "scale";

    @Override
    public boolean isApplicable(final EStructuralFeature feature) {
        return feature instanceof EAttribute && AsmUtils.getExtensionAnnotationListByName(feature, CONSTRAINTS).stream()
                .anyMatch(constraint -> constraint.getDetails().containsKey(PRECISION_CONSTRAINT_NAME));
    }

    @Override
    public Collection<ValidationResult> validateValue(Payload instance, final EStructuralFeature feature, final Object value, final Map<String, Object> context) {
        final Collection<ValidationResult> validationResults = new ArrayList<>();

        final String precisionString = AsmUtils.getExtensionAnnotationListByName(feature, CONSTRAINTS).stream()
                .map(constraint -> constraint.getDetails().get(PRECISION_CONSTRAINT_NAME))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Invalid maxLength constraint"));
        final String scaleString = AsmUtils.getExtensionAnnotationListByName(feature, CONSTRAINTS).stream()
                .map(constraint -> constraint.getDetails().get(SCALE_CONSTRAINT_NAME))
                .findAny()
                .orElse(null);

        final int precision = Integer.parseInt(precisionString);
        final int scale = scaleString != null ? Integer.parseInt(scaleString) : 0;

        if (value instanceof Number) {
            if (value instanceof Float) {
                validationResults.addAll(validateDecimal(instance, feature, context, precision, scale, BigDecimal.valueOf((Float) value)));
            } else if (value instanceof Double) {
                validationResults.addAll(validateDecimal(instance, feature, context, precision, scale, BigDecimal.valueOf((Double) value)));
            } else if (value instanceof Short) {
                validationResults.addAll(validateInteger(instance, feature, context, precision, BigInteger.valueOf((Short) value)));
            } else if (value instanceof Integer) {
                validationResults.addAll(validateInteger(instance, feature, context, precision, BigInteger.valueOf((Integer) value)));
            } else if (value instanceof Long) {
                validationResults.addAll(validateInteger(instance, feature, context, precision, BigInteger.valueOf((Long) value)));
            } else if (value instanceof BigInteger) {
                validationResults.addAll(validateInteger(instance, feature, context, precision, (BigInteger) value));
            } else if (value instanceof BigDecimal) {
                validationResults.addAll(validateDecimal(instance, feature, context, precision, scale, (BigDecimal) value));
            }
        } else {
            throw new IllegalStateException("Precision/scale constraints are supported on Number types only");
        }

        return validationResults;
    }

    private Collection<ValidationResult> validateInteger(final Payload instance, final EStructuralFeature feature, final Map<String, Object> context, final int precision, final BigInteger number) {
        final Collection<ValidationResult> validationResults = new ArrayList<>();

        final String string = number.toString().replaceAll("\\D*", "");
        if (precision < string.length()) {
            addValidationError(ImmutableMap.of(
                            FEATURE_KEY, DefaultPayloadValidator.ATTRIBUTE_TO_MODEL_TYPE.apply((EAttribute) feature),
                            PRECISION_CONSTRAINT_NAME, precision,
                            VALUE_KEY, number,
                            DefaultPayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(DefaultPayloadValidator.REFERENCE_ID_KEY))
                    ),
                    context.get(DefaultPayloadValidator.LOCATION_KEY),
                    validationResults,
                    ERROR_PRECISION_VALIDATION_FAILED);
        }

        return validationResults;
    }

    private Collection<ValidationResult> validateDecimal(final Payload instance, final EStructuralFeature feature, final Map<String, Object> context, final int precision, final int scale, final BigDecimal number) {
        final Collection<ValidationResult> validationResults = new ArrayList<>();

        final String string = number.toString().replaceAll("\\D*", "");
        final String fraction = number.toString().replaceAll(".*\\.\\D*", "");
        if (precision < string.length()) {
            addValidationError(ImmutableMap.of(
                            FEATURE_KEY, DefaultPayloadValidator.ATTRIBUTE_TO_MODEL_TYPE.apply((EAttribute) feature),
                            PRECISION_CONSTRAINT_NAME, precision,
                            VALUE_KEY, number,
                            DefaultPayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(DefaultPayloadValidator.REFERENCE_ID_KEY))
                    ),
                    context.get(DefaultPayloadValidator.LOCATION_KEY),
                    validationResults,
                    ERROR_PRECISION_VALIDATION_FAILED);
        }

        if (scale < fraction.length()) {
            addValidationError(ImmutableMap.of(
                            FEATURE_KEY, DefaultPayloadValidator.ATTRIBUTE_TO_MODEL_TYPE.apply((EAttribute) feature),
                            PRECISION_CONSTRAINT_NAME, precision,
                            SCALE_CONSTRAINT_NAME, fraction,
                            VALUE_KEY, number,
                            DefaultPayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(DefaultPayloadValidator.REFERENCE_ID_KEY))
                    ),
                    context.get(DefaultPayloadValidator.LOCATION_KEY),
                    validationResults,
                    ERROR_SCALE_VALIDATION_FAILED);
        }

        return validationResults;
    }
}
