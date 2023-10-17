package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

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

import hu.blackbelt.judo.meta.query.Filter;
import hu.blackbelt.judo.meta.query.OrderBy;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsResultSet;
import lombok.RequiredArgsConstructor;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class SubSelectMapper<ID> extends RdbmsMapper<SubSelect> {

    @Override
    public Stream<RdbmsResultSet<ID>> map(final SubSelect subSelect, RdbmsBuilderContext builderContext) {
        final RdbmsBuilder<?> rdbmsBuilder = builderContext.getRdbmsBuilder();
        final SubSelect parentIdFilterQuery = builderContext.getParentIdFilterQuery();
        final Map<String, Object> queryParameters = builderContext.getQueryParameters();

        final Object container = subSelect.eContainer();
        final boolean withoutFeatures = container instanceof Filter || container instanceof OrderBy || subSelect.getSelect().isAggregated();
        return Collections.singleton(
                RdbmsResultSet.<ID>builder()
                        .level(builderContext.getLevel() + 1)
                        .query(subSelect)
                        .filterByInstances(false)
                        .parentIdFilterQuery(parentIdFilterQuery)
                        .rdbmsBuilder((RdbmsBuilder<ID>) rdbmsBuilder)
                        .seek(null)
                        .withoutFeatures(withoutFeatures)
                        .mask(null)
                        .queryParameters(queryParameters)
                        .skipParents(false)
                        .build())
                .stream();
    }
}
