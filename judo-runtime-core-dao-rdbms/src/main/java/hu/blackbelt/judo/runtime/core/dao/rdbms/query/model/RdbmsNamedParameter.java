package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model;

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

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.mapper.api.Coercer;
import lombok.experimental.SuperBuilder;
import org.eclipse.emf.common.util.EMap;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

/**
 * RDBMS constant definition.
 */
@SuperBuilder
public class RdbmsNamedParameter extends RdbmsField {

    private RdbmsParameterMapper.Parameter parameter;

    private int index;

    @Override
    public String toSql(SqlConverterContext context) {
        final MapSqlParameterSource sqlParameters = context.sqlParameters;
        final boolean includeAlias = context.includeAlias;

        final String parameterName = "p" + index;

        final String sql;
        if (parameter.getValue() == null) {
            sql = "NULL";
        } else {
            sqlParameters.addValue(parameterName, parameter.getValue(), parameter.getSqlType(), parameter.getRdbmsTypeName());
            sql = cast(":" + parameterName, parameter.getRdbmsTypeName(), targetAttribute);
        }
        return getWithAlias(sql, includeAlias);
    }
}
