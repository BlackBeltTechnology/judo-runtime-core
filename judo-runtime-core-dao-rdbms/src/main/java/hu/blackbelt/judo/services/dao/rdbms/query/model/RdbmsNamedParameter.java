package hu.blackbelt.judo.services.dao.rdbms.query.model;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.services.dao.rdbms.RdbmsParameterMapper;
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
    public String toSql(final String prefix, final boolean includeAlias, final Coercer coercer, final MapSqlParameterSource sqlParameters, final EMap<Node, String> prefixes) {
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