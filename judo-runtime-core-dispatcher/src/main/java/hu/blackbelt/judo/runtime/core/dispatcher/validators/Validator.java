package hu.blackbelt.judo.runtime.core.dispatcher.validators;

import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultDispatcher;
import hu.blackbelt.judo.runtime.core.dispatcher.RequestConverter;
import hu.blackbelt.judo.runtime.core.exception.FeedbackItem;
import org.eclipse.emf.ecore.EParameter;
import org.eclipse.emf.ecore.EStructuralFeature;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface Validator {
    String ERROR_MISSING_REQUIRED_PARAMETER = "MISSING_REQUIRED_PARAMETER";
    String ERROR_NULL_PARAMETER_ITEM_IS_NOT_SUPPORTED = "NULL_PARAMETER_ITEM_IS_NOT_SUPPORTED";
    String ERROR_TOO_MANY_PARAMETERS = "TOO_MANY_PARAMETERS";
    String ERROR_TOO_FEW_PARAMETERS = "TOO_FEW_PARAMETERS";
    String ERROR_MISSING_IDENTIFIER_OF_BOUND_OPERATION = "MISSING_IDENTIFIER_OF_BOUND_OPERATION";
    String ERROR_INVALID_IDENTIFIER = "INVALID_IDENTIFIER";
    String ERROR_BOUND_OPERATION_INSTANCE_NOT_FOUND = "BOUND_OPERATION_INSTANCE_NOT_FOUND";
    String ERROR_ACCESS_DENIED_INVALID_TYPE = "ACCESS_DENIED_INVALID_TYPE";
    String ERROR_BOUND_OPERATION_INSTANCE_IS_IMMUTABLE = "BOUND_OPERATION_INSTANCE_IS_IMMUTABLE";
    String ERROR_TOO_FEW_ITEMS = "TOO_FEW_ITEMS";
    String ERROR_TOO_MANY_ITEMS = "TOO_MANY_ITEMS";
    String ERROR_MISSING_REQUIRED_ATTRIBUTE = "MISSING_REQUIRED_ATTRIBUTE";
    String ERROR_MISSING_REQUIRED_RELATION = "MISSING_REQUIRED_RELATION";
    String ERROR_INVALID_CONTENT = "INVALID_CONTENT";
    String ERROR_NULL_ITEM_IS_NOT_SUPPORTED = "NULL_ITEM_IS_NOT_SUPPORTED";
    String ERROR_CONVERSION_FAILED = "CONVERSION_FAILED";
    String ERROR_INVALID_FILE_TOKEN = "INVALID_FILE_TOKEN";

    String SIZE_PARAMETER = "size";

    String CONSTRAINTS = "constraints";

    String FEATURE_KEY = "feature";
    String VALUE_KEY = "value";

    boolean isApplicable(EStructuralFeature feature);

    Collection<FeedbackItem> validateValue(Payload payload, EStructuralFeature feature, Object value, Map<String, Object> context);


    public static void addValidationError(Map<String, Object> parameters,
                                          Object location,
                                          List<FeedbackItem> feedbackItems,
                                          String code) {
        final Map<String, Object> details = new LinkedHashMap<>();

        if (parameters != null) {
            details.putAll(parameters);
        }
        feedbackItems.add(FeedbackItem.builder()
                .code(code)
                .level(FeedbackItem.Level.ERROR)
                .location(location)
                .details(details)
                .build());
    }

}
