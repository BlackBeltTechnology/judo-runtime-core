package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.mapper.api.Coercer;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.eclipse.emf.common.util.EMap;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

@SuperBuilder
public class RdbmsCustomJoin extends RdbmsJoin {

    @NonNull
    private final String sql;

    @NonNull
    private final String sourceIdSetParameterName;

    @Override
    protected String getTableNameOrSubQuery(final String prefix, final Coercer coercer, final MapSqlParameterSource sqlParameters, final EMap<Node, String> prefixes) {
        return "(" + sql + ")";
    }
}
