package hu.blackbelt.judo.runtime.core.dao.rdbms;

import lombok.*;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EReference;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.Map;

public interface RdbmsParameterMapper<ID> {

    String getIdClassName();

    int getIdSqlType();

    Parameter createParameter(final Object originalValue, final EDataType dataType, final String name);

    Parameter createParameter(final Object originalValue, final EAttribute attribute);

    void mapAttributeParameters(final MapSqlParameterSource namedParameters, final Map<EAttribute, Object> attributeMap);

    void mapReferenceParameters(final MapSqlParameterSource namedParameters, final Map<EReference, ID> referenceMap);

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
