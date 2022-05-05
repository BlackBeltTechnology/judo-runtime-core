package hu.blackbelt.judo.services.dao.rdbms;

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.rdbmsDataTypes.TypeMapping;
import hu.blackbelt.mapper.api.Coercer;
import lombok.*;
import org.eclipse.emf.common.notify.Notifier;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EReference;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.sql.Time;
import java.time.*;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.Math.max;

@RequiredArgsConstructor
public class RdbmsParameterMapper {

    @NonNull
    private final Coercer coercer;

    @NonNull
    private final RdbmsModel rdbmsModel;

    @NonNull
    @Getter
    private IdentifierProvider identifierProvider;

    @NonNull
    private Dialect dialect;

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
        if (value instanceof BigDecimal) {
            final int precision = ((BigDecimal) value).precision();
            final int scale = ((BigDecimal) value).scale();

            typeName = "DECIMAL(" + (precision > scale ? precision : scale + 1) + "," + max(scale, 0) + ")";
        } else if (value instanceof BigInteger) {
            final int precision = ((BigInteger) value).toString(10).length();

            typeName = "NUMERIC(" + precision + ", 0)";
        } else if (value instanceof Boolean) {
            typeName = "BOOLEAN";
        } else if ((value instanceof Timestamp || value instanceof OffsetDateTime || value instanceof ZonedDateTime) || (dataType != null && AsmUtils.isTimestamp(dataType))) {
            if (Dialect.HSQLDB.equals(dialect)) {
                typeName = "TIMESTAMP WITH TIME ZONE";
            } else if (Dialect.POSTGRESQL.equals(dialect)) {
                typeName = "TIMESTAMPTZ";
            } else if (Dialect.JOOQ.equals(dialect)) {
                typeName = "TIMESTAMPTZ";
            } else {
                typeName = "TIMESTAMP";
            }
        } else if ((value instanceof LocalDateTime || value instanceof Instant) || (dataType != null && AsmUtils.isTimestamp(dataType))) {
            if (Dialect.HSQLDB.equals(dialect)) {
                typeName = "TIMESTAMP";
            } else if (Dialect.POSTGRESQL.equals(dialect)) {
                typeName = "TIMESTAMP";
            } else if (Dialect.JOOQ.equals(dialect)) {
                typeName = "TIMESTAMP";
            } else {
                typeName = "TIMESTAMP";
            }
        } else if ((value instanceof Time || value instanceof OffsetTime) || (dataType != null && AsmUtils.isTime(dataType))) {
            if (Dialect.HSQLDB.equals(dialect)) {
                typeName = "TIMESTAMP WITH TIME ZONE";
            } else if (Dialect.POSTGRESQL.equals(dialect)) {
                typeName = "TIMETZ";
            } else if (Dialect.JOOQ.equals(dialect)) {
                typeName = "TIME";
            } else {
                typeName = "TIME WITH TIME ZONE";}
        } else if (value instanceof LocalTime && (dataType != null && AsmUtils.isTime(dataType))) {
            typeName = "TIME";
        } else if ((value instanceof java.sql.Date || value instanceof Date || value instanceof LocalDate) || (dataType != null && AsmUtils.isDate(dataType))) {
            typeName = "DATE";
        } else if (value instanceof String) {
            if (Dialect.POSTGRESQL.equals(dialect)) {
                typeName = "TEXT";
            } else if (Dialect.HSQLDB.equals(dialect)) {
                typeName = "LONGVARCHAR";
            } else {
                typeName = "VARCHAR(2000)";
            }
        } else if (value instanceof Short) {
            typeName = "NUMERIC(5,0)";
        } else if (value instanceof Integer) {
            typeName = "NUMERIC(10,0)";
        } else if (value instanceof Long) {
            typeName = "NUMERIC(20,0)";
        } else if (value instanceof Float) {
            typeName = "FLOAT";
        } else if (value instanceof Double) {
            if (Dialect.POSTGRESQL.equals(dialect)) {
                typeName = "DOUBLE PRECISION";
            } else {
                typeName = "DOUBLE";
            }
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

    public <ID> void mapReferenceParameters(final MapSqlParameterSource namedParameters, final Map<EReference, ID> referenceMap) {
        final String targetType = getIdClassName();
        final int sqlType = getIdSqlType();
        referenceMap.entrySet().stream().forEach(e -> {
            namedParameters.addValue(e.getKey().getName(), coercer.coerce(e.getValue(), targetType), sqlType);
        });
    }

    private <T> Stream<T> all(final Class<T> clazz) {
        final Iterable<Notifier> asmContents = rdbmsModel.getResourceSet()::getAllContents;
        return StreamSupport.stream(asmContents.spliterator(), true)
                .filter(e -> clazz.isAssignableFrom(e.getClass())).map(e -> (T) e);
    }

    /**
     * SQL parameter
     */
    @Getter
    @Builder
    @ToString
    public static class Parameter {

        private final String name;

        private final Object value;

        /**
         * Java class for the given SQL parameter.
         */
        private final String javaTypeName;

        /**
         * SQL type used by JDBC driver, {@link java.sql.Types}.
         */
        private final Integer sqlType;

        /**
         * Type name for target RDBMS.
         */
        private final String rdbmsTypeName;
    }
}
