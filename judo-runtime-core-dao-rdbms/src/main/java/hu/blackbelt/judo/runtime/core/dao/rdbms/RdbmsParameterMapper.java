package hu.blackbelt.judo.runtime.core.dao.rdbms;

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

public interface RdbmsParameterMapper {

    String getIdClassName();

    int getIdSqlType();

    Parameter createParameter(final Object originalValue, final EDataType dataType, final String name);

    Parameter createParameter(final Object originalValue, final EAttribute attribute);

    void mapAttributeParameters(final MapSqlParameterSource namedParameters, final Map<EAttribute, Object> attributeMap);

    <ID> void mapReferenceParameters(final MapSqlParameterSource namedParameters, final Map<EReference, ID> referenceMap);

    int getSqlType(final String targetType);

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

    @AllArgsConstructor
    class ValueAndDataType {
        Object value;
        EDataType dataType;
    }



}
