package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model;

import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import org.eclipse.emf.common.util.*;
import org.eclipse.emf.ecore.EClass;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class RdbmsNavigationFilter extends RdbmsField {

    private final Filter filter;

    private final EMap<Node, EList<EClass>> ancestors = ECollections.asEMap(new HashMap<>());

    private final String from;

    private final List<RdbmsJoin> joins = new ArrayList<>();
    private final List<RdbmsField> conditions = new ArrayList<>();

    @Builder
    private RdbmsNavigationFilter(final Filter filter, final RdbmsBuilder rdbmsBuilder, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
        this.filter = filter;

        from = rdbmsBuilder.getTableName(filter.getType());

        joins.addAll(filter.getJoins().stream()
                .flatMap(subJoin -> subJoin.getAllJoins().stream()
                        .flatMap(j -> rdbmsBuilder.processJoin(j, ancestors, parentIdFilterQuery, rdbmsBuilder, true, null, queryParameters).stream())
                        .collect(Collectors.toList()).stream())
                .collect(Collectors.toList()));

        conditions.addAll(rdbmsBuilder.mapFeatureToRdbms(filter.getFeature(), ancestors, parentIdFilterQuery, queryParameters)
                .collect(Collectors.toList()));

        if (ancestors.containsKey(filter)) {
            ancestors.get(filter).forEach(ancestor ->
                    joins.addAll(rdbmsBuilder.getAdditionalJoins(filter, ancestors, joins)));
        }

        joins.addAll(filter.getSubSelects().stream()
                .filter(subSelect -> subSelect.getSelect().isAggregated())
                .map(subSelect -> {
                    final boolean group;
                    if (!subSelect.getNavigationJoins().isEmpty()) {
                        Node n = subSelect.getContainer();
                        final EList<Node> nodes = new BasicEList<>();
                        while (n != null) {
                            nodes.add(n);
                            if (n instanceof SubSelectJoin) {
                                nodes.add(((SubSelectJoin) n).getSubSelect().getBase());
                            }
                            n = (Node) n.eContainer();
                        }
                        group = subSelect.getNavigationJoins().get(0).getPartner() != null && nodes.contains(subSelect.getNavigationJoins().get(0).getPartner());
                    } else {
                        group = false;
                    }

                    final RdbmsQueryJoin queryJoin = RdbmsQueryJoin.builder()
                            .resultSet(
                                    RdbmsResultSet.builder()
                                            .query(subSelect)
                                            .filterByInstances(false)
                                            .parentIdFilterQuery(parentIdFilterQuery)
                                            .rdbmsBuilder(rdbmsBuilder)
                                            .seek(null)
                                            .withoutFeatures(true)
                                            .mask(null)
                                            .queryParameters(queryParameters)
                                            .skipParents(false)
                                            .build()
                            )
                            .outer(true)
                            .columnName(RdbmsAliasUtil.getOptionalParentIdColumnAlias(filter))
                            .partnerTable(group ? subSelect.getBase() : null)
                            .partnerColumnName(group ? StatementExecutor.ID_COLUMN_NAME : null)
                            .alias(subSelect.getAlias())
                            .build();
                    return queryJoin;
                })
                .collect(Collectors.toList()));
    }

    @Override
    public String toSql(String prefix, boolean includeAlias, Coercer coercer, MapSqlParameterSource sqlParameters, final EMap<Node, String> prefixes) {
        final String filterPrefix = RdbmsAliasUtil.getFilterPrefix(prefix);

        final EMap<Node, String> newPrefixes = new BasicEMap<>();
        newPrefixes.putAll(prefixes);
        newPrefixes.put(filter, filterPrefix);

        final String partnerAlias;
        if (filter.eContainer() instanceof SubSelect && !(((SubSelect) filter.eContainer()).getNavigationJoins().isEmpty())) {
            partnerAlias = RdbmsAliasUtil.AGGREGATE_PREFIX + ((Node) filter.eContainer()).getAlias();
        } else if (filter.eContainer() instanceof SubSelect && (((SubSelect) filter.eContainer()).getNavigationJoins().isEmpty())) {
            partnerAlias = ((SubSelect) filter.eContainer()).getSelect().getAlias();
        } else {
            partnerAlias = ((Node) filter.eContainer()).getAlias();
        }

        checkArgument(from != null || joins.size() < 2, "Size of JOINs must be at most 1 if FROM is not set");

        final String sql = //"-- " + newPrefixes.stream().map(p -> p.getKey().getAlias() + ": " + p.getValue()).collect(Collectors.joining(", ")) + "\n" +
                "SELECT 1 " +
                (from != null ? " FROM " + from + " AS " + filterPrefix + filter.getAlias() : "") +
                joins.stream().map(j -> j.toSql(filterPrefix, coercer, sqlParameters, newPrefixes, from == null)).collect(Collectors.joining()) +
                "\nWHERE " + prefix + partnerAlias + "." + StatementExecutor.ID_COLUMN_NAME + " = " + filterPrefix + filter.getAlias() + "." + StatementExecutor.ID_COLUMN_NAME +
                joins.stream().flatMap(j -> j.conditionToSql(filterPrefix, coercer, sqlParameters, newPrefixes).stream().map(c -> " AND " + c)).collect(Collectors.joining()) +
                conditions.stream().map(c -> " AND " + c.toSql(filterPrefix, false, coercer, sqlParameters, newPrefixes)).collect(Collectors.joining());
        return sql;
    }
}
