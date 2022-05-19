package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.emf.common.util.EMap;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

@Getter
@Builder
public class RdbmsOrderBy {

    @NonNull
    private final RdbmsField rdbmsField;

    private final boolean fromSubSelect;

    @NonNull
    private final Boolean descending;

    public String toSql(final String prefix, final Coercer coercer, final MapSqlParameterSource sqlParameters, final EMap<Node, String> prefixes) {
        return (fromSubSelect ? rdbmsField.getRdbmsAlias() : rdbmsField.toSql(prefix, false, coercer, sqlParameters, prefixes)) +
                (descending ? " DESC" : " ASC") + " NULLS " + (descending ? "FIRST" : "LAST");
    }
}
