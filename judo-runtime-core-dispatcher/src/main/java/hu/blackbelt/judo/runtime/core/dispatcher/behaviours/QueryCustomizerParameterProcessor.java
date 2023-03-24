package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

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
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.exception.ValidationException;
import hu.blackbelt.judo.runtime.core.dispatcher.querymask.QueryMaskStringParser;
import hu.blackbelt.mapper.api.Coercer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EEnum;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

@RequiredArgsConstructor
@Slf4j
public class QueryCustomizerParameterProcessor<ID> {

    public static final String THIS_NAME = "this";

    @NonNull
    private final AsmUtils asmUtils;

    @NonNull
    private final Boolean caseInsensitiveLike;

    @NonNull
    private final IdentifierProvider<ID> identifierProvider;

    @NonNull
    private final Coercer coercer;

    public static final Map<Integer, String> JQL_STRING_OPERATORS = ImmutableMap.<Integer, String>builder()
            .put(0, "<")
            .put(1, ">")
            .put(2, "<=")
            .put(3, ">=")
            .put(4, "==")
            .put(5, "!=")
            .put(6, "~")
            .put(7, "like")
            .build();
    public static final Map<Integer, String> JQL_NUMERIC_OPERATORS = ImmutableMap.<Integer, String>builder()
            .put(0, "<")
            .put(1, ">")
            .put(2, "<=")
            .put(3, ">=")
            .put(4, "==")
            .put(5, "!=")
            .build();
    public static final Map<Integer, String> JQL_BOOLEAN_OPERATORS = ImmutableMap.<Integer, String>builder()
            .put(0, "==")
            .build();
    public static final Map<Integer, String> JQL_ENUMERATION_OPERATORS = ImmutableMap.<Integer, String>builder()
            .put(0, "==")
            .put(1, "!=")
            .build();

    @SuppressWarnings("rawtypes")
    public DAO.QueryCustomizer build(final Map<String, Object> queryCustomizerParameter, final EClass clazz) {
        return DAO.QueryCustomizer.<ID>builder()
                .filter(extractFilteringParameter(clazz, queryCustomizerParameter))
                .orderByList(extractOrderingParameter(clazz, queryCustomizerParameter))
                .seek(extractSeekParameter(queryCustomizerParameter))
                .mask(extractMaskParameter(clazz, queryCustomizerParameter))
                .instanceIds(getIdentifiers(queryCustomizerParameter))
                .build();
    }

    @SuppressWarnings("unchecked")
    private String extractFilteringParameter(final EClass clazz, final Map<String, Object> queryCustomizerParameter) {
        final EList<EAttribute> attributes = clazz.getEAllAttributes();

        if (queryCustomizerParameter != null) {
            final List<String> filterConditions = new ArrayList<>();
            filterConditions.addAll(attributes.stream()
                    .flatMap(attribute ->
                            queryCustomizerParameter.get(attribute.getName()) != null
                                    ? ((List<Map<String, Object>>) queryCustomizerParameter.get(attribute.getName())).stream()
                                    .map(filter -> convertFilterToJql(attribute, (Integer) filter.get("operator"), filter.get("value"), caseInsensitiveLike))
                                    .filter(condition -> condition != null)
                                    : Collections.<String>emptyList().stream())
                    .collect(Collectors.toList()));

            return !filterConditions.isEmpty() ? filterConditions.stream().collect(Collectors.joining(" and ")) : null;
        }

        return null;
    }

    public static String convertFilterToJql(final EAttribute attribute, final Integer operator, final Object value, final boolean caseInsensitiveLike) {
        if (AsmUtils.isNumeric(attribute.getEAttributeType())) {
            final String jqlOperator = JQL_NUMERIC_OPERATORS.get(operator);
            checkArgument(jqlOperator != null, "Invalid numeric operator: " + operator);
            checkArgument(value instanceof Number, "Value must be a number");

            final Optional<String> measure = AsmUtils.getExtensionAnnotationCustomValue(attribute, "constraints", "measure", false);
            final Optional<String> unit = AsmUtils.getExtensionAnnotationCustomValue(attribute, "constraints", "unit", false);

            return THIS_NAME + "." + attribute.getName() + jqlOperator + value + unit.map(u -> " [" + measure.map(m -> m.replaceAll("\\.", "::") + "#").orElse("") + u + "]").orElse("");
        } else if (AsmUtils.isBoolean(attribute.getEAttributeType())) {
            final String jqlOperator = JQL_BOOLEAN_OPERATORS.get(operator);
            checkArgument(jqlOperator != null, "Invalid boolean operator: " + operator);
            checkArgument(value instanceof Boolean, "Value must be a boolean");
            return (Boolean.TRUE.equals(value) ? "" : "not ") + THIS_NAME + "." + attribute.getName();
        } else if (AsmUtils.isString(attribute.getEAttributeType())) {
            final String jqlOperator = JQL_STRING_OPERATORS.get(operator);
            checkArgument(jqlOperator != null, "Invalid string operator: " + operator);
            checkArgument(value instanceof String, "Value must be a string");
            final String escaped = ((String) value).replace("\"", "\\\"");
            switch (jqlOperator) {
                case "~":
                    return THIS_NAME + "." + attribute.getName() + "!matches(\"" + escaped + "\")";
                case "like":
                    return THIS_NAME + "." + attribute.getName() + "!" + (caseInsensitiveLike ? "ilike" : "like") + "(\"" + escaped + "\")";
                default:
                    return THIS_NAME + "." + attribute.getName() + jqlOperator + "\"" + escaped + "\"";
            }
        } else if (AsmUtils.isDate(attribute.getEAttributeType())) {
            final String jqlOperator = JQL_NUMERIC_OPERATORS.get(operator);
            checkArgument(jqlOperator != null, "Invalid date operator: " + operator);
            checkArgument(value instanceof LocalDate, "Value must be a local date");
            final String formattedDate = ((LocalDate) value).format(DateTimeFormatter.ISO_DATE);
            return THIS_NAME + "." + attribute.getName() + jqlOperator + "`" + formattedDate + "`";
        } else if (AsmUtils.isTimestamp(attribute.getEAttributeType())) {
            final String jqlOperator = JQL_NUMERIC_OPERATORS.get(operator);
            checkArgument(jqlOperator != null, "Invalid timestamp operator: " + operator);
            final String formattedTimestamp;
            if (value instanceof LocalDateTime) {
                formattedTimestamp = ((LocalDateTime) value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            } else if (value instanceof OffsetDateTime) {
                formattedTimestamp = ((OffsetDateTime) value).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            } else {
                throw new IllegalArgumentException("Value must be an instance of " + LocalDateTime.class.getName() + " or " + OffsetDateTime.class.getName() + ": " + value.getClass().getName());
            }
            return THIS_NAME + "." + attribute.getName() + jqlOperator + "`" + formattedTimestamp + "`";
        } else if (AsmUtils.isTime(attribute.getEAttributeType())) {
            final String jqlOperator = JQL_NUMERIC_OPERATORS.get(operator);
            checkArgument(jqlOperator != null, "Invalid time operator: " + operator);
            checkArgument(value instanceof LocalTime, "Value must be a local time");
            final String formattedTime = ((LocalTime) value).format(DateTimeFormatter.ISO_LOCAL_TIME);
            return THIS_NAME + "." + attribute.getName() + jqlOperator + "`" + formattedTime + "`";
        } else if (AsmUtils.isEnumeration(attribute.getEAttributeType())) {
            final EEnum enumeration = ((EEnum) attribute.getEAttributeType());
            final String jqlOperator = JQL_ENUMERATION_OPERATORS.get(operator);
            checkArgument(jqlOperator != null, "Invalid enumeration operator: " + operator);
            final String enumerationMember = enumeration.getELiterals().stream()
                    .filter(l -> Objects.equals(l.getValue(), value))
                    .map(l -> l.getName())
                    .findAny()
                    .orElseThrow(() -> new IllegalArgumentException("Invalid enumeration member: " + value));
            return THIS_NAME + "." + attribute.getName() + jqlOperator + AsmUtils.getClassifierFQName(enumeration).replace(".", "::") + "#" + enumerationMember;
        } else {
            log.info("Filtering is not supported on type: {}", AsmUtils.getAttributeFQName(attribute));
            return null;
        }
    }

    private List<DAO.OrderBy> extractOrderingParameter(final EClass clazz, final Map<String, Object> queryCustomizerParameter) {
        if (queryCustomizerParameter != null) {
            @SuppressWarnings("unchecked")
            final List<Map<String, Object>> orderByParameter = (List<Map<String, Object>>) queryCustomizerParameter.get("_orderBy");
            if (orderByParameter != null) {
                return orderByParameter.stream()
                        .map(p -> DAO.OrderBy.builder()
                                .attribute(resolveAttribute(clazz, (Integer) p.get("attribute")))
                                .descending((Boolean) p.getOrDefault("descending", Boolean.FALSE))
                                .build())
                        .collect(Collectors.toList());
            }
        }

        return Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    private DAO.Seek extractSeekParameter(final Map<String, Object> queryCustomizerParameter) {
        if (queryCustomizerParameter != null && queryCustomizerParameter.get("_seek") != null) {
            final Map<String, Object> _seek = (Map<String, Object>) queryCustomizerParameter.get("_seek");
            return DAO.Seek.builder()
                    .limit((Integer) _seek.get("limit"))
                    .offset(_seek.get("offset") != null ? (Integer) _seek.get("offset") : -1)
                    .lastItem(_seek.get("lastItem") != null ? Payload.asPayload((Map<String, Object>) _seek.get("lastItem")) : null)
                    .reverse(_seek.get("reverse") != null ? (Boolean) _seek.get("reverse") : false)
                    .build();
        }

        return null;
    }

    private Map<String, Object> extractMaskParameter(final EClass clazz, final Map<String, Object> queryCustomizerParameter) {
        if (queryCustomizerParameter != null && queryCustomizerParameter.get("_mask") != null) {
            try {
                final String _mask = (String) queryCustomizerParameter.get("_mask");
                return QueryMaskStringParser.parseQueryMask(clazz, _mask);
            } catch (RuntimeException ex) {
                final Map<String, Object> details = new LinkedHashMap<>();
                details.put("message", ex.getMessage());
                throw new ValidationException("Invalid query mask", Collections.singleton(ValidationResult.builder()
                        .code("INVALID_QUERY_MASK")
                        .level(ValidationResult.Level.ERROR)
                        .location("_mask")
                        .details(details)
                        .build()));
            }
        }

        return null;
    }

    private Collection<ID> getIdentifiers(final Map<String, Object> queryCustomizerParameter) {
        try {
            if (queryCustomizerParameter != null && queryCustomizerParameter.get("_identifier") != null) {
                final Object id = queryCustomizerParameter.get("_identifier");
                if (id instanceof Collection) {
                    return ((Collection<?>) id).stream()
                            .map(i -> coercer.coerce(i, identifierProvider.getType()))
                            .collect(Collectors.toList());
                } else {
                    return Collections.singleton(coercer.coerce(id, identifierProvider.getType()));
                }
            } else {
                return null;
            }
        } catch (RuntimeException ex) {
            final Map<String, Object> details = new LinkedHashMap<>();
            details.put("message", ex.getMessage());
            throw new ValidationException("Invalid identifier in filter", Collections.singleton(ValidationResult.builder()
                    .code("INVALID_IDENTIFIER_IN_QUERY_CUSTOMIZER")
                    .level(ValidationResult.Level.ERROR)
                    .location("_identifier")
                    .details(details)
                    .build()));
        }
    }

    private EAttribute resolveAttribute(final EClass type, final Integer ordinal) {
        final String enumOfAttributes = asmUtils.getModel()
                .map(modelName -> AsmUtils.getPackageFQName(type.getEPackage())
                        .replaceFirst(modelName.getName() + "._default_transferobjecttypes", modelName.getName())
                        .replaceFirst(modelName.getName(), modelName.getName() + "._extension") + "._OrderingAttribute" + type.getName())
                .orElseThrow(() -> new IllegalStateException("Unable to resolve enumeration generated for transfer object attributes"));

        return asmUtils.all(EEnum.class)
                .filter(e -> Objects.equals(AsmUtils.getClassifierFQName(e), enumOfAttributes))
                .flatMap(e -> e.getELiterals().stream().filter(l -> Objects.equals(ordinal, l.getValue())))
                .findAny()
                .map(l -> type.getEAllAttributes().stream().filter(a -> Objects.equals(a.getName(), l.getName()))
                        .findAny()
                        .orElseThrow(() -> new IllegalStateException("Unable to resolve enumeration member"))
                ).get();
    }
}
