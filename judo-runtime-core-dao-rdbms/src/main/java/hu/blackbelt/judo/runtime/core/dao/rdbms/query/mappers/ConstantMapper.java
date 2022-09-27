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

import hu.blackbelt.judo.meta.query.Constant;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsConstant;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.util.EcoreUtil;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class ConstantMapper<ID> extends RdbmsMapper<Constant> {

    @NonNull
    private final RdbmsBuilder<ID> rdbmsBuilder;

    @Override
    public Stream<? extends RdbmsField> map(final Constant constant, final EMap<Node, EList<EClass>> ancestors, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
        final String id = EcoreUtil.getIdentification(constant);
        if (id != null) {
            synchronized (this) {
                if (rdbmsBuilder.getConstantFields().get().containsKey(id)) {
                    return rdbmsBuilder.getConstantFields().get().get(id).stream();
                } else {
                    final List<? extends RdbmsField> fields = getFields(constant).collect(Collectors.toList());
                    rdbmsBuilder.getConstantFields().get().put(id, fields);
                    return fields.stream();
                }
            }
        } else {
            return getFields(constant);
        }
    }

    private Stream<? extends RdbmsField> getFields(final Constant constant) {
        return getTargets(constant).map(t -> RdbmsConstant.builder()
                .parameter(rdbmsBuilder.getParameterMapper().createParameter(constant.getValue(), null))
                .index(rdbmsBuilder.getConstantCounter().getAndIncrement())
                .target(t.getTarget())
                .targetAttribute(t.getTargetAttribute())
                .alias(t.getAlias())
                .build());
    }
}
