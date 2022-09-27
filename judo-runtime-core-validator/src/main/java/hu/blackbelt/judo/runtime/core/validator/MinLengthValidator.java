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

import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dao.api.ValidationResult;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EStructuralFeature;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public class MinLengthValidator implements Validator {

    private static final String CONSTRAINT_NAME = "minLength";

    @Override
    public boolean isApplicable(final EStructuralFeature feature) {
        return feature instanceof EAttribute && AsmUtils.getExtensionAnnotationListByName(feature, CONSTRAINTS).stream()
                .anyMatch(constraint -> constraint.getDetails().containsKey(CONSTRAINT_NAME));
    }

    @Override
    public Collection<ValidationResult> validateValue(Payload instance, final EStructuralFeature feature, final Object value, final Map<String, Object> context) {
        final Collection<ValidationResult> validationResults = new ArrayList<>();

        final String minLengthString = AsmUtils.getExtensionAnnotationListByName(feature, CONSTRAINTS).stream()
                .map(constraint -> constraint.getDetails().get(CONSTRAINT_NAME))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Invalid minLength constraint"));

        final int minLength = Integer.parseInt(minLengthString);

        if (value instanceof String) {
            final int length = ((String) value).length();
            if (length < minLength) {
                Validator.addValidationError(ImmutableMap.of(
                                FEATURE_KEY, DefaultPayloadValidator.ATTRIBUTE_TO_MODEL_TYPE.apply((EAttribute) feature),
                                CONSTRAINT_NAME, minLength,
                                VALUE_KEY, value,
                                DefaultPayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(DefaultPayloadValidator.REFERENCE_ID_KEY))
                        ),
                        context.get(DefaultPayloadValidator.LOCATION_KEY),
                        validationResults,
                        ERROR_MIN_LENGTH_VALIDATION_FAILED);
            }
        } else {
            throw new IllegalStateException("MinLength constraint is supported on String type only");
        }

        return validationResults;
    }
}
