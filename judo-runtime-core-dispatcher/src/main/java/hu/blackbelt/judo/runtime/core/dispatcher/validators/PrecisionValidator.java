package hu.blackbelt.judo.runtime.core.dispatcher.validators;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.exception.FeedbackItem;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultDispatcher;
import hu.blackbelt.judo.runtime.core.dispatcher.RequestConverter;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EStructuralFeature;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class PrecisionValidator implements Validator {

    private static final String PRECISION_CONSTRAINT_NAME = "precision";
    private static final String SCALE_CONSTRAINT_NAME = "precision";

    @Override
    public boolean isApplicable(final EStructuralFeature feature) {
        return feature instanceof EAttribute && AsmUtils.getExtensionAnnotationListByName(feature, CONSTRAINTS).stream()
                .anyMatch(constraint -> constraint.getDetails().containsKey(PRECISION_CONSTRAINT_NAME));
    }

    @Override
    public Collection<FeedbackItem> validateValue(Payload instance, final EStructuralFeature feature, final Object value, final Map<String, Object> context) {
        final Collection<FeedbackItem> feedbackItems = new ArrayList<>();

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
                feedbackItems.addAll(validateDecimal(instance, feature, context, precision, scale, BigDecimal.valueOf((Float) value)));
            } else if (value instanceof Double) {
                feedbackItems.addAll(validateDecimal(instance, feature, context, precision, scale, BigDecimal.valueOf((Double) value)));
            } else if (value instanceof Short) {
                feedbackItems.addAll(validateInteger(instance, feature, context, precision, BigInteger.valueOf((Short) value)));
            } else if (value instanceof Integer) {
                feedbackItems.addAll(validateInteger(instance, feature, context, precision, BigInteger.valueOf((Integer) value)));
            } else if (value instanceof Long) {
                feedbackItems.addAll(validateInteger(instance, feature, context, precision, BigInteger.valueOf((Long) value)));
            } else if (value instanceof BigInteger) {
                feedbackItems.addAll(validateInteger(instance, feature, context, precision, (BigInteger) value));
            } else if (value instanceof BigDecimal) {
                feedbackItems.addAll(validateDecimal(instance, feature, context, precision, scale, (BigDecimal) value));
            }
        } else {
            throw new IllegalStateException("Precision/scale constraints are supported on Number types only");
        }

        return feedbackItems;
    }

    private Collection<FeedbackItem> validateInteger(final Payload instance, final EStructuralFeature feature, final Map<String, Object> context, final int precision, final BigInteger number) {
        final Collection<FeedbackItem> feedbackItems = new ArrayList<>();

        final String string = number.toString().replaceAll("[^0-9]*", "");
        if (precision < string.length()) {
            final Map<String, Object> details = new LinkedHashMap<>();
            details.put(FEATURE_KEY, RequestConverter.ATTRIBUTE_TO_MODEL_TYPE.apply((EAttribute) feature));
            details.put(PRECISION_CONSTRAINT_NAME, precision);
            details.put(VALUE_KEY, number);
            if (instance.containsKey(DefaultDispatcher.REFERENCE_ID_KEY)) {
                details.put(DefaultDispatcher.REFERENCE_ID_KEY, instance.get(DefaultDispatcher.REFERENCE_ID_KEY));
            }
            feedbackItems.add(FeedbackItem.builder()
                    .code("PRECISION_VALIDATION_FAILED")
                    .level(FeedbackItem.Level.ERROR)
                    .location(context.get(RequestConverter.LOCATION_KEY))
                    .details(details)
                    .build());
        }

        return feedbackItems;
    }

    private Collection<FeedbackItem> validateDecimal(final Payload instance, final EStructuralFeature feature, final Map<String, Object> context, final int precision, final int scale, final BigDecimal number) {
        final Collection<FeedbackItem> feedbackItems = new ArrayList<>();

        final String string = number.toString().replaceAll("[^0-9]*", "");
        final String fraction = number.toString().replaceAll(".*\\.[^0-9]*", "");
        if (precision < string.length()) {
            final Map<String, Object> details = new LinkedHashMap<>();
            details.put(FEATURE_KEY, RequestConverter.ATTRIBUTE_TO_MODEL_TYPE.apply((EAttribute) feature));
            details.put(PRECISION_CONSTRAINT_NAME, precision);
            details.put(VALUE_KEY, number);
            if (instance.containsKey(DefaultDispatcher.REFERENCE_ID_KEY)) {
                details.put(DefaultDispatcher.REFERENCE_ID_KEY, instance.get(DefaultDispatcher.REFERENCE_ID_KEY));
            }
            feedbackItems.add(FeedbackItem.builder()
                    .code("PRECISION_VALIDATION_FAILED")
                    .level(FeedbackItem.Level.ERROR)
                    .location(context.get(RequestConverter.LOCATION_KEY))
                    .details(details)
                    .build());
        }

        if (scale < fraction.length()) {
            final Map<String, Object> details = new LinkedHashMap<>();
            details.put(FEATURE_KEY, RequestConverter.ATTRIBUTE_TO_MODEL_TYPE.apply((EAttribute) feature));
            details.put(PRECISION_CONSTRAINT_NAME, precision);
            details.put(SCALE_CONSTRAINT_NAME, fraction);
            details.put(VALUE_KEY, number);
            if (instance.containsKey(DefaultDispatcher.REFERENCE_ID_KEY)) {
                details.put(DefaultDispatcher.REFERENCE_ID_KEY, instance.get(DefaultDispatcher.REFERENCE_ID_KEY));
            }
            feedbackItems.add(FeedbackItem.builder()
                    .code("SCALE_VALIDATION_FAILED")
                    .level(FeedbackItem.Level.ERROR)
                    .location(context.get(RequestConverter.LOCATION_KEY))
                    .details(details)
                    .build());
        }

        return feedbackItems;
    }
}
