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
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EReference;

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
    protected String getTableNameOrSubQuery(SqlConverterContext converterContext) {
        aliasToCompareWith = converterContext.getPrefix() + alias;
        return tableName;
    }

    @Override
    protected String getJoinCondition(SqlConverterContext converterContext) {
        final List<String> partners = new ArrayList<>();
        final String prefix = converterContext.getPrefix();
        for (int index = 0; index < references.size(); index++) {
            partners.add(prefix + alias + POSTFIX + index);
        }

        checkArgument(!partners.isEmpty(), "Partner must not be empty");

        joinConditionTableAliases.addAll(partners);
        aliasToCompareWith = prefix + alias;
        if (partners.size() == 1) {
            return partners.get(0) + "." + partnerColumnName + " = " + prefix + alias + "." + columnName;
        } else {
            return "COALESCE(" + partners.stream()
                    .map(p -> p + "." + partnerColumnName)
                    .collect(Collectors.joining(",")) + ") = " + prefix + alias + "." + columnName;
        }
    }
}
