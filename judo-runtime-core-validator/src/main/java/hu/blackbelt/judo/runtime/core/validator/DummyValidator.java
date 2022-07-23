package hu.blackbelt.judo.runtime.core.validator;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dao.api.ValidationResult;
import org.eclipse.emf.ecore.EStructuralFeature;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class DummyValidator implements Validator {
    @Override
    public boolean isApplicable(EStructuralFeature feature) {
        return false;
    }

    @Override
    public Collection<ValidationResult> validateValue(Payload payload, EStructuralFeature feature, Object value, Map<String, Object> context) {
        return Collections.emptyList();
    }
}
