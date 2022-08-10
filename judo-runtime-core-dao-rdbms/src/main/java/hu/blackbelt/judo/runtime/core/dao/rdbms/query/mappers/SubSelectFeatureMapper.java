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

import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsColumn;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;

import java.util.Map;
import java.util.stream.Stream;

public class SubSelectFeatureMapper extends RdbmsMapper<SubSelectFeature> {

    @Override
    public Stream<RdbmsField> map(final SubSelectFeature feature, final EMap<Node, EList<EClass>> ancestors, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
        final String pattern;
        if ((feature.getFeature() instanceof Function) && ((Function) feature.getFeature()).getSignature() == FunctionSignature.COUNT) {
            pattern = "COALESCE({0}.{1}, 0)";
        } else {
            pattern = null;
        }

        return getTargets(feature).map(t -> RdbmsColumn.builder()
                .target(t.getTarget())
                .targetAttribute(t.getTargetAttribute())
                .partnerTable(feature.getSubSelect())
                .columnName(getTargets(feature.getFeature()).map(tt -> tt.getAlias() + (tt.getTarget() != null ? "_" + tt.getTarget().getIndex() : "")).findAny().get())
                .pattern(pattern)
                .alias(t.getAlias())
                .build());
    }
}
