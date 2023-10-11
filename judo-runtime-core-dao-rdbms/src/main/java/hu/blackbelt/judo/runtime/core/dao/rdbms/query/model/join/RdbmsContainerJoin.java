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
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.SqlConverterContext;
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
    protected String getTableNameOrSubQuery(SqlConverterContext contexts) {
        aliasToCompareWith = contexts.prefix + alias;
        return tableName;
    }

    @Override
    protected String getJoinCondition(SqlConverterContext context) {
        final List<String> partners = new ArrayList<>();
        for (int index = 0; index < references.size(); index++) {
            partners.add(context.prefix + alias + POSTFIX + index);
        }

        checkArgument(!partners.isEmpty(), "Partner must not be empty");

        joinConditionTableAliases.addAll(partners);
        aliasToCompareWith = context.prefix + alias;
        if (partners.size() == 1) {
            return partners.get(0) + "." + partnerColumnName + " = " + context.prefix + alias + "." + columnName;
        } else {
            return "COALESCE(" + partners.stream().map(p -> p + "." + partnerColumnName).collect(Collectors.joining(",")) + ") = " + context.prefix + alias + "." + columnName;
        }
    }
}
