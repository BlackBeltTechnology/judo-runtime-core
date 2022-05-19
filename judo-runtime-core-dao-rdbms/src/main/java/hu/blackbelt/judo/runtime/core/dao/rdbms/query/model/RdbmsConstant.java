package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.mapper.api.Coercer;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.EMap;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Types;

/**
 * RDBMS constant definition.
 */
@SuperBuilder
@Slf4j
public class RdbmsConstant extends RdbmsField {

    private RdbmsParameterMapper.Parameter parameter;

    private int index;

    @Override
    public String toSql(final String prefix, final boolean includeAlias, final Coercer coercer, final MapSqlParameterSource sqlParameters, final EMap<Node, String> prefixes) {
        final String sql;
        if (parameter.getSqlType() == Types.VARCHAR || parameter.getSqlType() == Types.CHAR || parameter.getSqlType() == Types.NVARCHAR) {
            final String value = coercer.coerce(parameter.getValue(), String.class);
            sql = "'" + (value != null ? value.replace("'", "''") : "NULL") + "'";
        } else if (parameter.getSqlType() == Types.BIGINT || parameter.getSqlType() == Types.DECIMAL || parameter.getSqlType() == Types.DOUBLE
                || parameter.getSqlType() == Types.FLOAT || parameter.getSqlType() == Types.INTEGER || parameter.getSqlType() == Types.SMALLINT
                || parameter.getSqlType() == Types.TINYINT || parameter.getSqlType() == Types.REAL || parameter.getSqlType() == Types.NUMERIC) {
            final BigDecimal value = coercer.coerce(parameter.getValue(), BigDecimal.class);
            sql = value != null ? value.toPlainString() : "NULL";
        } else if (parameter.getSqlType() == Types.TIMESTAMP) {
            final String value = coercer.coerce(parameter.getValue(), String.class).replaceAll("Z$", "").replace("T", " ");
            sql = "CAST(" + (value != null ? "'" + value + "'" : "NULL") + " AS " + parameter.getRdbmsTypeName() + ")";
        } else if (parameter.getSqlType() == Types.TIMESTAMP_WITH_TIMEZONE) {
            final String value = coercer.coerce(parameter.getValue(), String.class).replaceAll("Z$", "+00:00").replaceAll("T", " ");
            sql = "CAST(" + (value != null ? "'" + value + "'" : "NULL") + " AS " + parameter.getRdbmsTypeName() + ")";
        } else if (parameter.getSqlType() == Types.TIME) {
            final String value = coercer.coerce(parameter.getValue(), String.class);
            sql = "CAST(" + (value != null ? "'" + value + "'" : "NULL") + " AS " + parameter.getRdbmsTypeName() + ")";
        } else if (parameter.getSqlType() == Types.DATE) {
            final Date value = coercer.coerce(parameter.getValue(), Date.class);
            sql = "CAST(" + (value != null ? "'" + value + "'" : "NULL") + " AS " + parameter.getRdbmsTypeName() + ")";
        } else if (parameter.getSqlType() == Types.BOOLEAN || parameter.getSqlType() == Types.BIT) {
            final Boolean value = coercer.coerce(parameter.getValue(), Boolean.class);
            if (value == null) {
                sql = "NULL";
            } else if (value) {
                sql = "(1 = 1)";
            } else {
                sql = "(1 = 0)";
            }
        } else if (parameter.getSqlType() == Types.BINARY || parameter.getSqlType() == Types.OTHER) {
            final String value = coercer.coerce(parameter.getValue(), String.class);
            sql = "'" + (value != null ? value.replace("'", "''") : "NULL") + "'";
        } else {
            log.warn("Unsupported constant type: {}, replaced with SQL named parameter", parameter);

            final String parameterName = "c" + index;
            if (parameter.getValue() == null) {
                sql = "NULL";
            } else {
                sqlParameters.addValue(parameterName, parameter.getValue(), parameter.getSqlType(), parameter.getRdbmsTypeName());
                sql = cast(":" + parameterName, parameter.getRdbmsTypeName(), targetAttribute);
            }
        }
        return getWithAlias(sql, includeAlias);
    }
}