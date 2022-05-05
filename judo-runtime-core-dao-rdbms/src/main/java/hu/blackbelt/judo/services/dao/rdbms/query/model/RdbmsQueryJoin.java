package hu.blackbelt.judo.services.dao.rdbms.query.model;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.mapper.api.Coercer;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.eclipse.emf.common.util.EMap;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.Map;

@SuperBuilder
public class RdbmsQueryJoin extends RdbmsJoin {

    @NonNull
    private final RdbmsResultSet resultSet;

    @Override
    protected String getTableNameOrSubQuery(final String prefix, final Coercer coercer, final MapSqlParameterSource sqlParameters, final EMap<Node, String> prefixes) {
        return "(" + resultSet.toSql(prefix, true, coercer, sqlParameters, prefixes) + ")";
    }
}
