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
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dao.api.ValidationResult;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EStructuralFeature;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.runtime.core.validator.DefaultPayloadValidator.GLOBAL_VALIDATION_CONTEXT;
import static hu.blackbelt.judo.runtime.core.validator.DefaultPayloadValidator.LOCATION_KEY;

@Slf4j
public class UniqueAttributeValidator<ID> implements Validator {

    public static final String THIS_NAME = "this";
    public static final String UNIQUE_ATTRIBUTE_VALIDATOR_CONTEXT = "uniqueAttributeValidatorContext";

    @NonNull
    private final DAO<ID> dao;

    @NonNull
    private final AsmUtils asmUtils;

    @NonNull
    private final IdentifierProvider<ID> identifierProvider;

    @NonNull
    Context context;

    public UniqueAttributeValidator(@NonNull DAO<ID> dao, @NonNull AsmModel asmModel, @NonNull IdentifierProvider<ID> identifierProvider, @NonNull Context context) {
        this.dao = dao;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());
        this.identifierProvider = identifierProvider;
        this.context = context;
    }

    @Override
    public boolean isApplicable(final EStructuralFeature feature) {
        return feature instanceof EAttribute &&
                asmUtils.getMappedAttribute((EAttribute) feature).isPresent() &&
                AsmUtils.isIdentifier(asmUtils.getMappedAttribute((EAttribute) feature).get());
    }

    @Override
    public Collection<ValidationResult> validateValue(Payload instance, final EStructuralFeature feature, final Object value, final Map<String, Object> validationContext) {

        Map<String, Object> globalValidationContext = (Map<String, Object>) validationContext.get(GLOBAL_VALIDATION_CONTEXT);

        UniqueValidationContextInfo uniqueValidationContextInfo;
        if (globalValidationContext.containsKey(UNIQUE_ATTRIBUTE_VALIDATOR_CONTEXT)) {
            uniqueValidationContextInfo = (UniqueValidationContextInfo) globalValidationContext.get(UNIQUE_ATTRIBUTE_VALIDATOR_CONTEXT);
        } else {
            uniqueValidationContextInfo = new UniqueValidationContextInfo();
            globalValidationContext.put(UNIQUE_ATTRIBUTE_VALIDATOR_CONTEXT, uniqueValidationContextInfo);
        }

        final Collection<ValidationResult> validationResults = new ArrayList<>();

        if (!(feature instanceof EAttribute)
                || !asmUtils.getMappedAttribute((EAttribute) feature).isPresent()
                || !AsmUtils.isIdentifier(asmUtils.getMappedAttribute((EAttribute) feature).get())) {
            return validationResults;
        }

        final EAttribute mappedAttribute = asmUtils.getMappedAttribute((EAttribute) feature).get();
        final ID originalId = instance.getAs(identifierProvider.getType(), identifierProvider.getName());

        // When the attribute have to be inserted, heck the current value already presented in the insertable values
        // If not present, insert it.
        if (originalId == null) {
            if (!uniqueValidationContextInfo.insertableUniqueValue.containsKey(mappedAttribute)) {
                uniqueValidationContextInfo.insertableUniqueValue.put(mappedAttribute, new HashSet<>());
                uniqueValidationContextInfo.insertableUniqueValue.get(mappedAttribute).add(value);
            } else if (uniqueValidationContextInfo.insertableUniqueValue.get(mappedAttribute).contains(value)) {
                Validator.addValidationError(ImmutableMap.of(
                                FEATURE_KEY, DefaultPayloadValidator.ATTRIBUTE_TO_MODEL_TYPE.apply((EAttribute) feature),
                                VALUE_KEY, value
                        ),
                        validationContext.get(LOCATION_KEY),
                        validationResults,
                        ERROR_IDENTIFIER_ATTRIBUTE_UNIQUENESS_VIOLATION);
            } else {
                uniqueValidationContextInfo.insertableUniqueValue.get(mappedAttribute).add(value);
            }
        }

        // Check in the persisted values
        final String filter = convertFilterToJql(mappedAttribute, value);
        final List<Payload> queryResult = dao.search(feature.getEContainingClass(), DAO.QueryCustomizer.<ID>builder()
                .filter(filter)
                .mask(ImmutableMap.of(mappedAttribute.getName(), true))
                .seek(DAO.Seek.builder()
                        .limit(2)
                        .build())
                .build());

        if (queryResult.size() > 0) {
            Set<ID> sameValuesIds = queryResult.stream()
                    .map(i -> i.getAs(identifierProvider.getType(), identifierProvider.getName()))
                    .collect(Collectors.toSet());

            if (originalId == null || !sameValuesIds.contains(originalId) || queryResult.size() > 1) {
                Validator.addValidationError(ImmutableMap.of(
                                FEATURE_KEY, DefaultPayloadValidator.ATTRIBUTE_TO_MODEL_TYPE.apply((EAttribute) feature),
                                VALUE_KEY, value
                        ),
                        validationContext.get(LOCATION_KEY),
                        validationResults,
                        ERROR_IDENTIFIER_ATTRIBUTE_UNIQUENESS_VIOLATION);
            }
        }

        return validationResults;
    }

    private String convertFilterToJql(final EAttribute attribute, final Object value) {
        if (AsmUtils.isNumeric(attribute.getEAttributeType())) {
            checkArgument(value instanceof Number, "Value must be a number");
            final Optional<String> measure = AsmUtils.getExtensionAnnotationCustomValue(attribute, "constraints", "measure", false);
            final Optional<String> unit = AsmUtils.getExtensionAnnotationCustomValue(attribute, "constraints", "unit", false);
            return THIS_NAME + "." + attribute.getName() + "==" + value + unit.map(u -> " [" + measure.map(m -> m.replaceAll("\\.", "::") + "#").orElse("") + u + "]").orElse("");
        } else if (AsmUtils.isBoolean(attribute.getEAttributeType())) {
            checkArgument(value instanceof Boolean, "Value must be a boolean");
            return (Boolean.TRUE.equals(value) ? "" : "not ") + THIS_NAME + "." + attribute.getName();
        } else if (AsmUtils.isString(attribute.getEAttributeType())) {
            checkArgument(value instanceof String, "Value must be a string");
            final String escaped = ((String) value).replace("\"", "\\\"");
            return THIS_NAME + "." + attribute.getName() + "==" + "\"" + escaped + "\"";
        } else if (AsmUtils.isDate(attribute.getEAttributeType())) {
            checkArgument(value instanceof LocalDate, "Value must be a date");
            final String formattedDate = ((LocalDate) value).format(DateTimeFormatter.ISO_DATE);
            return THIS_NAME + "." + attribute.getName() + "==" + "`" + formattedDate + "`";
        } else if (AsmUtils.isTimestamp(attribute.getEAttributeType())) {
            final String formattedTimestamp;
            if (value instanceof LocalDateTime) {
                formattedTimestamp = ((LocalDateTime) value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            } else if (value instanceof OffsetDateTime) {
                formattedTimestamp = ((OffsetDateTime) value).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            } else {
                throw new IllegalArgumentException("Value must be an instance of " + LocalDateTime.class.getName() + " or " + OffsetDateTime.class.getName() + ": " + value.getClass().getName());
            }
            return THIS_NAME + "." + attribute.getName() + "==" + "`" + formattedTimestamp + "`";
        } else if (AsmUtils.isTime(attribute.getEAttributeType())) {
            checkArgument(value instanceof LocalTime, "Value must be a time");
            final String formattedTime = ((LocalTime) value).format(DateTimeFormatter.ISO_TIME);
            return THIS_NAME + "." + attribute.getName() + "==" + "`" + formattedTime + "`";
        } else if (AsmUtils.isEnumeration(attribute.getEAttributeType())) {
            final EEnum enumeration = ((EEnum) attribute.getEAttributeType());
            final String enumerationMember = enumeration.getELiterals().stream()
                    .filter(l -> Objects.equals(l.getValue(), value))
                    .map(l -> l.getName())
                    .findAny()
                    .orElseThrow(() -> new IllegalArgumentException("Invalid enumeration member: " + value));
            return THIS_NAME + "." + attribute.getName() + "==" + AsmUtils.getClassifierFQName(enumeration).replace(".", "::") + "#" + enumerationMember;
        } else {
            log.info("Filtering is not supported on type: {}", AsmUtils.getAttributeFQName(attribute));
            return null;
        }
    }


    private static class UniqueValidationContextInfo {
        Map<EAttribute, Set<Object>> insertableUniqueValue = new HashMap<>();
    }
}
