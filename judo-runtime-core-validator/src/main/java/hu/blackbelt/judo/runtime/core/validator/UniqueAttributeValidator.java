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
import org.eclipse.emf.ecore.EStructuralFeature;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class UniqueAttributeValidator<ID> implements Validator {

    public static final String THIS_NAME = "this";

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

        final Collection<ValidationResult> validationResults = new ArrayList<>();

        if (!(feature instanceof EAttribute)
                || !asmUtils.getMappedAttribute((EAttribute) feature).isPresent()
                || !AsmUtils.isIdentifier(asmUtils.getMappedAttribute((EAttribute) feature).get())) {
            return validationResults;
        }

        final EAttribute filterAttribute = asmUtils.getMappedAttribute((EAttribute) feature).get();

        final String filter = convertFilterToJql(filterAttribute, value);
        final List<Payload> queryResult = dao.search(feature.getEContainingClass(), DAO.QueryCustomizer.<ID>builder()
                .filter(filter)
                .mask(ImmutableMap.of(filterAttribute.getName(), true))
                .seek(DAO.Seek.builder()
                        .limit(2)
                        .build())
                .build());

        if (queryResult.size() > 0) {
            final ID originalId = instance.getAs(identifierProvider.getType(), identifierProvider.getName());
            Set<ID> sameValuesIds = queryResult.stream()
                    .map(i -> i.getAs(identifierProvider.getType(), identifierProvider.getName()))
                    .collect(Collectors.toSet());
            if (originalId == null || !sameValuesIds.contains(originalId) || queryResult.size() > 1) {

            }
        }

        /*
        final ID id = ((Payload) value).getAs(identifierProvider.getType(), identifierProvider.getName());
        if (id == null || !validIds.contains(id)) {
            Validator.addValidationError(ImmutableMap.of(
                            identifierProvider.getName(), id,
                            SIGNED_IDENTIFIER_KEY, Optional.ofNullable(((Payload) value).get(SIGNED_IDENTIFIER_KEY)),
                            REFERENCE_ID_KEY, Optional.ofNullable(instance.get(REFERENCE_ID_KEY))
                    ),
                    validationContext.get(LOCATION_KEY),
                    validationResults,
                    ERROR_NOT_ACCEPTED_BY_RANGE);
        } */

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
            final String formattedTimestamp = ((OffsetDateTime) value).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            return THIS_NAME + "." + attribute.getName() + "==" + "`" + formattedTimestamp + "`";
        } else if (AsmUtils.isTime(attribute.getEAttributeType())) {
            final String formattedTime = ((LocalTime) value).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            return THIS_NAME + "." + attribute.getName() + "==" + "`" + formattedTime + "`";
        } else if (AsmUtils.isTime(attribute.getEAttributeType())) {
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

}
