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
                validationResults.addAll(validateDecimal(instance, feature, context, precision, scale, new BigDecimal(Float.toString((Float) value))));
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

        if (precision < number.precision()) {
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

        if (scale < number.scale()) {
            addValidationError(ImmutableMap.of(
                            FEATURE_KEY, DefaultPayloadValidator.ATTRIBUTE_TO_MODEL_TYPE.apply((EAttribute) feature),
                            PRECISION_CONSTRAINT_NAME, precision,
                            SCALE_CONSTRAINT_NAME, number.scale(),
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
