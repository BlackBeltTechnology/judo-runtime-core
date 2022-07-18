package hu.blackbelt.judo.runtime.core.dao.core.validators;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.runtime.core.exception.FeedbackItem;
import org.eclipse.emf.ecore.EStructuralFeature;

import java.util.Collection;
import java.util.Map;

public interface Validator {

    String CONSTRAINTS = "constraints";

    String FEATURE_KEY = "feature";
    String VALUE_KEY = "value";

    boolean isApplicable(EStructuralFeature feature);

    Collection<FeedbackItem> validateValue(Payload payload, EStructuralFeature feature, Object value, Map<String, Object> context);
}
