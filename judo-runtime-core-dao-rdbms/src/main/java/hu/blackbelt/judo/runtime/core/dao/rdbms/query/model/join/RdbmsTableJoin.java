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
import hu.blackbelt.mapper.api.Coercer;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.EMap;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@SuperBuilder
public class RdbmsTableJoin extends RdbmsJoin {

    @NonNull
    private final String tableName;

    private final RdbmsTableJoin rdbmsPartnerTable;

    @Override
    protected String getTableNameOrSubQuery(final String prefix, final Coercer coercer, final MapSqlParameterSource sqlParameters, final EMap<Node, String> prefixes) {
        return tableName;
    }

    @Override
    protected String getJoinCondition(String prefix, EMap<Node, String> prefixes, Coercer coercer, MapSqlParameterSource sqlParameters) {
        if (rdbmsPartnerTable != null && partnerTable != null) {
            log.warn("Both rdbmsPartnerTable and partnerTable are set for RdbmsTableJoin, rdbmsPartnerTable will be used instead of partnerTable");
        }
        if (rdbmsPartnerTable != null) {
            final String joinCondition = prefix + rdbmsPartnerTable.alias + "." + rdbmsPartnerTable.columnName + " = " + prefix + alias + "." + columnName;
            joinConditionTableAliases.addAll(List.of(prefix + rdbmsPartnerTable.alias, prefix + alias));
            aliasToCompareWith = prefix + alias;

            if (!onConditions.isEmpty()) {
                return joinCondition + " AND " + onConditions.stream()
                                                             .map(c -> c.toSql(prefix, false, coercer, sqlParameters, prefixes))
                                                             .collect(Collectors.joining(" AND "));
            }

            return joinCondition;
        } else {
            return super.getJoinCondition(prefix, prefixes, coercer, sqlParameters);
        }
    }

}
