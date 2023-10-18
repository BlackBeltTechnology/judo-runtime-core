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
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.SqlConverterContext;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.EMap;

import java.util.*;
import java.util.stream.Collectors;

/**
 * RDBMS JOIN definition.
 */
@SuperBuilder
@NoArgsConstructor
@Slf4j
public abstract class RdbmsJoin {

    protected String columnName;

    @Getter @Setter
    protected Node partnerTable;
    protected String partnerTablePrefix;
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

    public String toSql(SqlConverterContext converterContext, final boolean fromIsEmpty) {
        final String joinCondition = getJoinCondition(converterContext);

        String table = getTableNameOrSubQuery(converterContext) + " AS " + converterContext.getPrefix() + alias;
        joinConditionTableAliases.add(converterContext.getPrefix() + alias);
        aliasToCompareWith = converterContext.getPrefix() + alias;
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
                partnerTableName = prefixes.get(partnerTable) +
                        (partnerTablePrefix != null ? partnerTablePrefix : "") +
                        partnerTable.getAlias() +
                        (partnerTablePostfix != null ? partnerTablePostfix : "");
            } else {
                partnerTableName = prefix + (partnerTablePrefix != null ? partnerTablePrefix : "") +
                        partnerTable.getAlias() +
                        (partnerTablePostfix != null ? partnerTablePostfix : "");
            }
        } else {
            partnerTableName = null;
        }

        if (partnerTableName != null) {
            joinConditionTableAliases.add(partnerTableName);
        }
        return partnerTableName;
    }

    protected String getJoinCondition(SqlConverterContext converterContext) {
        final String partnerTableName = getPartnerTableName(converterContext.getPrefix(), converterContext.getPrefixes());
        final String prefix = converterContext.getPrefix();

        if (log.isTraceEnabled()) {
            log.trace("Converter context:             " + converterContext.toString());
            log.trace("Junction table name:           " + junctionTableName);
            log.trace("Junction column name:          " + junctionColumnName);
            log.trace("Junction opposite column name: " + junctionOppositeColumnName);
            log.trace("Partner table name:            " + partnerTableName);
            log.trace("Partner column name:           " + partnerColumnName);

        }

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
                    .map(c -> c.toSql(converterContext.toBuilder()
                                    .includeAlias(false)
                                    .build()
                    )).collect(Collectors.joining(" AND "));
        } else {
            return joinCondition;
        }
    }

    public Collection<String> conditionToSql(SqlConverterContext converterContext) {
        return conditions.stream().map(c -> c.toSql(
                converterContext.toBuilder()
                        .includeAlias(false)
                        .build()
        )).collect(Collectors.toList());
    }

    protected abstract String getTableNameOrSubQuery(SqlConverterContext converterContext);
}
