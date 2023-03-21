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
