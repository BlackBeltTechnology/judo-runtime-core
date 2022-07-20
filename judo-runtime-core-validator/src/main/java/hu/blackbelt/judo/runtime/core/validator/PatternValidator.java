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
import java.util.Optional;
import java.util.regex.Pattern;

public class PatternValidator implements Validator {

    private static final String CONSTRAINT_NAME = "pattern";

    @Override
    public boolean isApplicable(final EStructuralFeature feature) {
        return feature instanceof EAttribute && AsmUtils.getExtensionAnnotationListByName(feature, CONSTRAINTS).stream()
                .anyMatch(constraint -> constraint.getDetails().containsKey(CONSTRAINT_NAME));
    }

    @Override
    public Collection<FeedbackItem> validateValue(Payload instance, final EStructuralFeature feature, final Object value, final Map<String, Object> context) {
        final Collection<FeedbackItem> feedbackItems = new ArrayList<>();

        final String patternString = AsmUtils.getExtensionAnnotationListByName(feature, CONSTRAINTS).stream()
                .map(constraint -> constraint.getDetails().get(CONSTRAINT_NAME))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Invalid maxLength constraint"));

        final Pattern pattern = Pattern.compile(patternString);

        if (value instanceof String) {
            if (!pattern.matcher((String)value).matches()) {
                Validator.addValidationError(ImmutableMap.of(
                                FEATURE_KEY, PayloadValidator.ATTRIBUTE_TO_MODEL_TYPE.apply((EAttribute) feature),
                                CONSTRAINT_NAME, pattern,
                                VALUE_KEY, value,
                                PayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(PayloadValidator.REFERENCE_ID_KEY))
                        ),
                        context.get(PayloadValidator.LOCATION_KEY),
                        feedbackItems,
                        ERROR_PATTERN_VALIDATION_FAILED);
            }
        } else {
            throw new IllegalStateException("Pattern constraint is supported on String type only");
        }

        return feedbackItems;
    }
}
