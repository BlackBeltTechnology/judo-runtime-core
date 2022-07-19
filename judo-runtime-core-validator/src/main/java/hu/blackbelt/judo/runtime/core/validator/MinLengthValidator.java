package hu.blackbelt.judo.runtime.core.validator;

import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.exception.FeedbackItem;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EStructuralFeature;

import java.util.ArrayList;
import java.util.Collection;
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
                Validator.addValidationError(ImmutableMap.of(
                                FEATURE_KEY, DaoPayloadValidator.ATTRIBUTE_TO_MODEL_TYPE.apply((EAttribute) feature),
                                CONSTRAINT_NAME, minLength,
                                VALUE_KEY, value,
                                DaoPayloadValidator.REFERENCE_ID_KEY, instance.get(DaoPayloadValidator.REFERENCE_ID_KEY)
                        ),
                        context.get(DaoPayloadValidator.LOCATION_KEY),
                        feedbackItems,
                        ERROR_MIN_LENGTH_VALIDATION_FAILED);
            }
        } else {
            throw new IllegalStateException("MinLength constraint is supported on String type only");
        }

        return feedbackItems;
    }
}
