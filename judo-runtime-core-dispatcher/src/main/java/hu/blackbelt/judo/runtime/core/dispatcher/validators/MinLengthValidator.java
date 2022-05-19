package hu.blackbelt.judo.runtime.core.dispatcher.validators;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.exception.FeedbackItem;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultDispatcher;
import hu.blackbelt.judo.runtime.core.dispatcher.RequestConverter;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EStructuralFeature;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class MinLengthValidator implements Validator {

    private static final String CONSTRAINT_NAME = "minLength";

    @Override
    public boolean isApplicable(final EStructuralFeature feature) {
        return feature instanceof EAttribute && AsmUtils.getExtensionAnnotationListByName(feature, CONSTRAINTS).stream()
                .anyMatch(constraint -> constraint.getDetails().containsKey(CONSTRAINT_NAME));
    }

    @Override
    public Collection<FeedbackItem> validateValue(Payload instance, final EStructuralFeature feature, final Object value, final Map<String, Object> context) {
        final Collection<FeedbackItem> feedbackItems = new ArrayList<>();

        final String minLengthString = AsmUtils.getExtensionAnnotationListByName(feature, CONSTRAINTS).stream()
                .map(constraint -> constraint.getDetails().get(CONSTRAINT_NAME))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Invalid minLength constraint"));

        final int minLength = Integer.parseInt(minLengthString);

        if (value instanceof String) {
            final int length = ((String) value).length();
            if (length < minLength) {
                final Map<String, Object> details = new LinkedHashMap<>();
                details.put(FEATURE_KEY, RequestConverter.ATTRIBUTE_TO_MODEL_TYPE.apply((EAttribute) feature));
                details.put(CONSTRAINT_NAME, minLength);
                details.put(VALUE_KEY, value);
                if (instance.containsKey(DefaultDispatcher.REFERENCE_ID_KEY)) {
                    details.put(DefaultDispatcher.REFERENCE_ID_KEY, instance.get(DefaultDispatcher.REFERENCE_ID_KEY));
                }
                feedbackItems.add(FeedbackItem.builder()
                        .code("MIN_LENGTH_VALIDATION_FAILED")
                        .level(FeedbackItem.Level.ERROR)
                        .location(context.get(RequestConverter.LOCATION_KEY))
                        .details(details)
                        .build());
            }
        } else {
            throw new IllegalStateException("MinLength constraint is supported on String type only");
        }

        return feedbackItems;
    }
}
