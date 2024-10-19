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

import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.SqlConverterContext;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@SuperBuilder
public class RdbmsTableJoin extends RdbmsJoin {

    @NonNull
    private final String tableName;

    private final RdbmsTableJoin rdbmsPartnerTable;

    @Override
    protected String getTableNameOrSubQuery(SqlConverterContext converterContext) {
        return tableName;
    }

    @Override
    protected String getJoinCondition(SqlConverterContext converterContext) {
        final String prefix = converterContext.getPrefix();
        if (rdbmsPartnerTable != null && partnerTable != null) {
            log.warn("Both rdbmsPartnerTable and partnerTable are set for RdbmsTableJoin, rdbmsPartnerTable will be used instead of partnerTable");
        }
        if (rdbmsPartnerTable != null) {
            final String joinCondition = prefix + rdbmsPartnerTable.alias + "." + rdbmsPartnerTable.columnName + " = " +
                    prefix + alias + "." + columnName;
            joinConditionTableAliases.addAll(List.of(prefix + rdbmsPartnerTable.alias, prefix + alias));
            aliasToCompareWith = prefix + alias;

            if (!onConditions.isEmpty()) {
                return joinCondition + " AND " +
                        onConditions.stream()
                                .map(c -> c.toSql(converterContext.toBuilder().includeAlias(false).build()))
                                .collect(Collectors.joining(" AND "));
            }

            return joinCondition;
        } else {
            return super.getJoinCondition(converterContext);
        }
    }

}
