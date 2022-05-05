package hu.blackbelt.judo.services.dao.rdbms.query.model;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Singular;
import lombok.experimental.SuperBuilder;
import org.eclipse.emf.common.util.EMap;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * RDBMS JOIN definition.
 */
@SuperBuilder
@NoArgsConstructor
public abstract class RdbmsJoin {
    protected String columnName;

    protected Node partnerTable;
    protected String partnerTablePrefix;
    protected String partnerTablePostfix;
    protected String partnerColumnName;

    protected String junctionTableName;
    protected String junctionColumnName;
    protected String junctionOppositeColumnName;

    @Getter
    protected String alias;

    protected boolean outer;

    @NonNull
    @Singular
    protected Collection<RdbmsField> conditions = new ArrayList<>();

    @NonNull
    @Singular
    protected Collection<RdbmsField> onConditions = new ArrayList<>();

    public String toSql(final String prefix, final Coercer coercer, final MapSqlParameterSource sqlParameters, final EMap<Node, String> prefixes, final boolean fromIsEmpty) {
        final String joinCondition = getJoinCondition(prefix, prefixes, coercer, sqlParameters);

        return (fromIsEmpty ? "\nFROM " : (outer ? "\nLEFT OUTER JOIN " : "\nJOIN ")) + getTableNameOrSubQuery(prefix, coercer, sqlParameters, prefixes) +
                " AS " + prefix + alias +
                (fromIsEmpty ? "" : " ON (" + joinCondition + ")");
    }

    protected String getPartnerTableName(final String prefix, final EMap<Node, String> prefixes) {
        final String partnerTableName;
        if (partnerTable != null) {
            if (prefixes.containsKey(partnerTable)) {
                partnerTableName = prefixes.get(partnerTable) + (partnerTablePrefix != null ? partnerTablePrefix : "") + partnerTable.getAlias() + (partnerTablePostfix != null ? partnerTablePostfix : "");
            } else {
                partnerTableName = prefix + (partnerTablePrefix != null ? partnerTablePrefix : "") + partnerTable.getAlias() + (partnerTablePostfix != null ? partnerTablePostfix : "");
            }
        } else {
            partnerTableName = null;
        }

        return partnerTableName;
    }

    protected String getJoinCondition(final String prefix, final EMap<Node, String> prefixes, final Coercer coercer, final MapSqlParameterSource sqlParameters) {
        final String partnerTableName = getPartnerTableName(prefix, prefixes);

        final String joinCondition;
        if (junctionTableName != null) {
            joinCondition = "EXISTS (SELECT 1 FROM " + junctionTableName + " WHERE " + partnerTableName + "." + partnerColumnName + " = " + junctionTableName + "." + junctionOppositeColumnName + " AND " + junctionTableName + "." + junctionColumnName + " = " + prefix + alias + "." + columnName + ")";
        } else if (partnerTableName != null && partnerColumnName != null && columnName != null) {
            joinCondition = partnerTableName + "." + partnerColumnName + " = " + prefix + alias + "." + columnName;
        } else {
            joinCondition = "1 = 1";
        }

        if (!onConditions.isEmpty()) {
            return joinCondition + " AND " + onConditions.stream()
                    .map(c -> c.toSql(prefix, false, coercer, sqlParameters, prefixes))
                    .collect(Collectors.joining(" AND "));
        } else {
            return joinCondition;
        }
    }

    public Collection<String> conditionToSql(final String prefix, final Coercer coercer, final MapSqlParameterSource sqlParameters, final EMap<Node, String> prefixes) {
        return conditions.stream().map(c -> c.toSql(prefix, false, coercer, sqlParameters, prefixes)).collect(Collectors.toList());
    }

    protected abstract String getTableNameOrSubQuery(String prefix, Coercer coercer, MapSqlParameterSource sqlParameters, EMap<Node, String> prefixes);
}