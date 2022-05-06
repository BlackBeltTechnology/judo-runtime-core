package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.mapper.api.Coercer;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EReference;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

@SuperBuilder
public class RdbmsContainerJoin extends RdbmsJoin {

    public static final String POSTFIX = "_c";

    @NonNull
    private final String tableName;

    @NonNull
    private final EList<EReference> references;

    @Override
    protected String getTableNameOrSubQuery(final String prefix, final Coercer coercer, final MapSqlParameterSource sqlParameters, final EMap<Node, String> prefixes) {
        return tableName;
    }

    @Override
    protected String getJoinCondition(String prefix, EMap<Node, String> prefixes, final Coercer coercer, final MapSqlParameterSource sqlParameters) {
        final List<String> partners = new ArrayList<>();
        int index = 0;
        for (final EReference r : references) {
            partners.add(prefix + alias + POSTFIX + index++);
        }

        checkArgument(!partners.isEmpty(), "Partner must not be empty");

        if (partners.size() == 1) {
            return partners.get(0) + "." + partnerColumnName + " = " + prefix + alias + "." + columnName;
        } else {
            return "COALESCE(" + partners.stream().map(p -> p + "." + partnerColumnName).collect(Collectors.joining(",")) + ") = " + prefix + alias + "." + columnName;
        }
    }
}
