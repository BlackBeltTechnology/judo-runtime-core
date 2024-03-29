package hu.blackbelt.judo.runtime.core.dao.rdbms;

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

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.rdbmsDataTypes.TypeMapping;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.types.RdbmsDecimalType;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.emf.common.notify.Notifier;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EReference;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.Math.max;

public abstract class DefaultRdbmsParameterMapper<ID> implements RdbmsParameterMapper<ID> {

    @NonNull
    private final Coercer coercer;

    @NonNull
    private final RdbmsModel rdbmsModel;

    @NonNull
    @Getter
    private IdentifierProvider<ID> identifierProvider;

    @Getter
    private Map<Class<?>, Predicate<ValueAndDataType>> typePredicates = new LinkedHashMap<>();

    @Getter
    private Map<Class<?>, Function<ValueAndDataType, String>> sqlTypes = new LinkedHashMap<>();

    public DefaultRdbmsParameterMapper(
            @NonNull Coercer coercer,
            @NonNull RdbmsModel rdbmsModel,
            @NonNull IdentifierProvider<ID> identifierProvider) {
        this.coercer = coercer;
        this.rdbmsModel = rdbmsModel;
        this.identifierProvider = identifierProvider;

        typePredicates.put(BigDecimal.class, vd -> vd.value instanceof BigDecimal);
        sqlTypes.put(BigDecimal.class, vd -> {
            final int precision = ((BigDecimal) vd.value).precision();
            final int scale = ((BigDecimal) vd.value).scale();
            return new RdbmsDecimalType(precision > scale ? precision : scale + 1, max(scale, 0)).toSql();
        });

        typePredicates.put(BigInteger.class, vd -> vd.value instanceof BigInteger);
        sqlTypes.put(BigInteger.class, vd -> {
            final int precision = ((BigInteger) vd.value).toString(10).length();
            return "NUMERIC(" + precision + ", 0)";
        });

        typePredicates.put(Boolean.class, vd -> vd.value instanceof Boolean);
        sqlTypes.put(Boolean.class, vd -> "BOOLEAN");

        typePredicates.put(Timestamp.class, vd -> (vd.value instanceof Timestamp
                    || vd.value instanceof OffsetDateTime
                    || vd.value instanceof ZonedDateTime)
                    || (vd.dataType != null && AsmUtils.isTimestamp(vd.dataType)));
        sqlTypes.put(Timestamp.class, vd -> "TIMESTAMP");

        typePredicates.put(LocalDateTime.class, vd -> (vd.value instanceof LocalDateTime
                    || vd.value instanceof Instant)
                    || (vd.dataType != null && AsmUtils.isTimestamp(vd.dataType)));
        sqlTypes.put(LocalDateTime.class, vd -> "TIMESTAMP");

        typePredicates.put(Time.class, vd -> (vd.value instanceof Time
                || vd.value instanceof OffsetTime)
                || (vd.dataType != null && AsmUtils.isTime(vd.dataType)));
        sqlTypes.put(Time.class, vd -> "TIME");


        typePredicates.put(LocalTime.class, vd -> vd.value instanceof LocalTime
                || (vd.dataType != null && AsmUtils.isTime(vd.dataType)));
        sqlTypes.put(LocalTime.class, vd -> "TIME");

        typePredicates.put(java.sql.Date.class, vd -> (
                vd.value instanceof java.sql.Date
                        || vd.value instanceof Date
                        || vd.value instanceof LocalDate)
                        || (vd.dataType != null && AsmUtils.isDate(vd.dataType)));
        sqlTypes.put(java.sql.Date.class, vd -> "DATE");

        typePredicates.put(String.class, vd -> vd.value instanceof String);
        sqlTypes.put(String.class, vd -> "VARCHAR(2000)");

        typePredicates.put(Short.class, vd -> vd.value instanceof Short);
        sqlTypes.put(Short.class, vd -> "NUMERIC(5,0)");

        typePredicates.put(Integer.class, vd -> vd.value instanceof Integer);
        sqlTypes.put(Integer.class, vd -> "NUMERIC(10,0)");

        typePredicates.put(Long.class, vd -> vd.value instanceof Long);
        sqlTypes.put(Long.class, vd -> "NUMERIC(20,0)");

        typePredicates.put(Float.class, vd -> vd.value instanceof Float);
        sqlTypes.put(Float.class, vd -> "FLOAT");

        typePredicates.put(Double.class, vd -> vd.value instanceof Double);
        sqlTypes.put(Double.class, vd -> "DOUBLE");

    }

    public String getIdClassName() {
        return rdbmsModel.getResource().getContents().stream()
                .filter(m -> m instanceof hu.blackbelt.judo.meta.rdbmsDataTypes.TypeMappings)
                .flatMap(m -> (((hu.blackbelt.judo.meta.rdbmsDataTypes.TypeMappings) m).getTypeMappings().stream()))
                .filter(m -> Objects.equals(identifierProvider.getType().getName(), m.getAsmType()))
                .map(m -> m.getRdbmsJdbcType())
                .findAny().get();
    }

    public int getIdSqlType() {
        return getSqlType(identifierProvider.getType().getName());
    }

    private String getClassName(final EDataType dataType) {
        if (dataType instanceof EEnum) {
            return Integer.class.getName();
        } else {
            final String result = all(TypeMapping.class)
                    .filter(m -> Objects.equals(dataType.getInstanceClassName(), m.getAsmType()))
                    .map(m -> m.getRdbmsJdbcType())
                    .findAny().orElse(String.class.getName());
            return result;
        }
    }

    private String getClassName(final String dataTypeName, final String defaultClassName) {
        final String result = all(TypeMapping.class)
                .filter(m -> Objects.equals(dataTypeName, m.getAsmType()))
                .map(m -> m.getRdbmsJdbcType())
                .findAny().orElse(defaultClassName);
        return result;
    }

    public int getSqlType(final String targetType) {
        return rdbmsModel.getResource().getContents().stream()
                .filter(m -> m instanceof hu.blackbelt.judo.meta.rdbmsDataTypes.TypeMappings)
                .flatMap(m -> (((hu.blackbelt.judo.meta.rdbmsDataTypes.TypeMappings) m).getTypeMappings().stream()))
                .filter(m -> Objects.equals(targetType, m.getAsmType()))
                .map(m -> m.getRdbmsSqlType())
                .findAny().get();
    }

    public Parameter createParameter(final Object originalValue, final EDataType dataType, final String name) {
        final String targetType;
        if (dataType != null) {
            targetType = getClassName(dataType);
        } else if (originalValue != null) {
            if (originalValue.getClass().isEnum()) {
                targetType = Integer.class.getName();
            } else {
                targetType = getClassName(originalValue.getClass().getName(), originalValue.getClass().getName());
            }
        } else {
            return Parameter.builder()
                    .value(null)
                    .build();
        }

        final int sqlType = getSqlType(targetType);

        final Object value = coercer.coerce(originalValue, targetType);
        final String typeName;

        ValueAndDataType vd = new ValueAndDataType(value, dataType);
        Optional<? extends Class<?>> resolvedType =
                typePredicates.entrySet().stream()
                        .filter(e -> e.getValue().test(vd)).map(e -> e.getKey()).findFirst();

        if (resolvedType.isPresent()) {
            typeName = sqlTypes.get(resolvedType.get()).apply(vd);
        } else {
            typeName = null;
        }

        return Parameter.builder()
                .name(name)
                .value(value)
                .sqlType(sqlType)
                .javaTypeName(targetType)
                .rdbmsTypeName(typeName)
                .build();
    }

    public Parameter createParameter(final Object originalValue, final EAttribute attribute) {
        if (attribute != null) {
            return createParameter(originalValue, attribute.getEAttributeType(), attribute.getName());
        } else {
            return createParameter(originalValue, null, null);
        }
    }

    public void mapAttributeParameters(final MapSqlParameterSource namedParameters, final Map<EAttribute, Object> attributeMap) {
        attributeMap.entrySet().stream().forEach(e -> {
            final Parameter parameter = createParameter(e.getValue(), e.getKey());
            namedParameters.addValue(parameter.getName(), parameter.getValue(), parameter.getSqlType(), parameter.getRdbmsTypeName());
        });
    }

    public void mapReferenceParameters(final MapSqlParameterSource namedParameters, final Map<EReference, ID> referenceMap) {
        final String targetType = getIdClassName();
        final int sqlType = getIdSqlType();
        referenceMap.entrySet().stream().forEach(e -> {
            namedParameters.addValue(e.getKey().getName(), coercer.coerce(e.getValue(), targetType), sqlType);
        });
    }

    @SuppressWarnings("unchecked")
    private <T> Stream<T> all(final Class<T> clazz) {
        final Iterable<Notifier> asmContents = rdbmsModel.getResourceSet()::getAllContents;
        return StreamSupport.stream(asmContents.spliterator(), true)
                .filter(e -> clazz.isAssignableFrom(e.getClass())).map(e -> (T) e);
    }
}
