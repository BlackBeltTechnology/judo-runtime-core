package hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join;

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
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import hu.blackbelt.mapper.api.Coercer;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.*;
import java.util.stream.Collectors;

/**
 * RDBMS JOIN definition.
 */
@SuperBuilder
@NoArgsConstructor
public abstract class RdbmsJoin {

    protected String columnName;

    @Getter @Setter
    protected Node partnerTable;
    @Getter
    protected EClass type;
    @Getter
    protected String partnerTablePrefix;
    @Getter
    protected String partnerTablePostfix;
    @Getter @Setter
    protected String partnerColumnName;

    protected String junctionTableName;
    protected String junctionColumnName;
    protected String junctionOppositeColumnName;

    @Getter
    protected String alias;

    @Getter
    protected String aliasToCompareWith;

    @Getter
    protected final Set<String> joinConditionTableAliases = new HashSet<>();

    @Getter @Setter
    protected boolean outer;

    @NonNull
    @Singular
    protected Collection<RdbmsField> conditions = new ArrayList<>();

    @NonNull
    @Singular
    protected Collection<RdbmsField> onConditions = new ArrayList<>();

    public String toSql(final String prefix, final Coercer coercer, final MapSqlParameterSource sqlParameters, final EMap<Node, String> prefixes, final boolean fromIsEmpty) {
        final String joinCondition = getJoinCondition(prefix, prefixes, coercer, sqlParameters);

        String table = getTableNameOrSubQuery(prefix, coercer, sqlParameters, prefixes) + " AS " + prefix + alias;
        joinConditionTableAliases.add(prefix + alias);
        aliasToCompareWith = prefix + alias;
        if (fromIsEmpty) {
            return "\nFROM " + table;
        } else {
            return (outer ? "\nLEFT OUTER JOIN " : "\nJOIN ") + table + " ON (" + joinCondition + ")";
        }
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

        if (partnerTableName != null) {
            joinConditionTableAliases.add(partnerTableName);
        }
        return partnerTableName;
    }

    protected String getJoinCondition(final String prefix, final EMap<Node, String> prefixes, final Coercer coercer, final MapSqlParameterSource sqlParameters) {
        final String partnerTableName = getPartnerTableName(prefix, prefixes);

        final String joinCondition;
        if (junctionTableName != null) {
            joinCondition = "EXISTS (" +
                            "   SELECT 1" +
                            "   FROM " + junctionTableName +
                            "   WHERE " + partnerTableName + "." + partnerColumnName + " = " + junctionTableName + "." + junctionOppositeColumnName +
                            "       AND " + junctionTableName + "." + junctionColumnName + " = " + prefix + alias + "." + columnName +
                            ")";
            joinConditionTableAliases.addAll(List.of(partnerTableName, junctionTableName, prefix + alias));
            aliasToCompareWith = prefix + alias;
        } else if (partnerTableName != null && partnerColumnName != null && columnName != null) {
            joinCondition = partnerTableName + "." + partnerColumnName + " = " + prefix + alias + "." + columnName;
            joinConditionTableAliases.addAll(List.of(partnerTableName, prefix + alias));
            aliasToCompareWith = prefix + alias;
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
