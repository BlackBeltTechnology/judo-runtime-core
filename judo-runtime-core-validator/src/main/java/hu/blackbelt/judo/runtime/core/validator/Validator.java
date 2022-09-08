package hu.blackbelt.judo.runtime.core.validator;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 * 
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 * 
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dao.api.ValidationResult;
import org.eclipse.emf.ecore.EStructuralFeature;

import java.util.*;

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
    String ERROR_PRECISION_VALIDATION_FAILED = "PRECISION_VALIDATION_FAILED";
    String ERROR_SCALE_VALIDATION_FAILED = "SCALE_VALIDATION_FAILED";
    String ERROR_MAX_LENGTH_VALIDATION_FAILED = "MAX_LENGTH_VALIDATION_FAILED";
    String ERROR_PATTERN_VALIDATION_FAILED = "PATTERN_VALIDATION_FAILED";
    String ERROR_MIN_LENGTH_VALIDATION_FAILED = "MIN_LENGTH_VALIDATION_FAILED";

    String ERROR_NOT_ACCEPTED_BY_RANGE = "NOT_ACCEPTED_BY_RANGE";

    String ERROR_IDENTIFIER_ATTRIBUTE_UNIQUENESS_VIOLATION = "IDENTIFIER_ATTRIBUTE_UNIQUENESS_VIOLATION";

    String SIZE_PARAMETER = "size";

    String CONSTRAINTS = "constraints";

    String FEATURE_KEY = "feature";
    String VALUE_KEY = "value";

    boolean isApplicable(EStructuralFeature feature);

    Collection<ValidationResult> validateValue(Payload payload, EStructuralFeature feature, Object value, Map<String, Object> context);


    static void addValidationError(Map<String, Object> parameters,
                                          Object location,
                                          Collection<ValidationResult> validationResults,
                                          String code) {
        final Map<String, Object> details = new LinkedHashMap<>();

        if (parameters != null) {
            parameters.forEach((k,v) -> {
                if (v instanceof Optional) {
                    if (((Optional) v).isPresent()) {
                        details.put(k, ((Optional) v).get());
                    }
                } else {
                    details.put(k, v);
                }
            });
        }
        validationResults.add(ValidationResult.builder()
                .code(code)
                .level(ValidationResult.Level.ERROR)
                .location(location)
                .details(details)
                .build());
    }

}
