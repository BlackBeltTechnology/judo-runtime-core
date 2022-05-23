package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Singular;
import lombok.experimental.SuperBuilder;
import org.eclipse.emf.common.util.EMap;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.text.MessageFormat;
import java.util.List;

/**
 * RDBMS function definition.
 */
@SuperBuilder
public class RdbmsFunction extends RdbmsField {

    private String pattern;

    @Singular
    private List<RdbmsField> parameters;

    @Override
    public String toSql(final String prefix, final boolean includeAlias, final Coercer coercer, final MapSqlParameterSource sqlParameters, final EMap<Node, String> prefixes) {
        final String sql = cast(MessageFormat.format(pattern, parameters.stream().map(p -> p.toSql(prefix, false, coercer, sqlParameters, prefixes)).toArray()), null, targetAttribute);
        return getWithAlias(sql, includeAlias);
    }
}