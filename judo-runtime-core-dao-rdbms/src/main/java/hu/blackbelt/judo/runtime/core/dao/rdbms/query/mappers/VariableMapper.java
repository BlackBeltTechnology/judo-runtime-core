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

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsNamedParameter;
import lombok.RequiredArgsConstructor;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.util.EcoreUtil;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class VariableMapper<ID> extends RdbmsMapper<Variable> {

    public static final String PARAMETER_VARIABLE_KEY = "PARAMETER";

    @Override
    public Stream<? extends RdbmsField> map(final Variable variable, RdbmsBuilderContext builderContext) {
        final RdbmsBuilder<?> rdbmsBuilder = builderContext.getRdbmsBuilder();

        final String id = EcoreUtil.getIdentification(variable);
        if (id != null) {
            synchronized (this) {
                if (rdbmsBuilder.getConstantFields().get().containsKey(id)) {
                    return rdbmsBuilder.getConstantFields().get().get(id).stream();
                } else {
                    final List<? extends RdbmsField> fields = getFields(builderContext, variable).collect(Collectors.toList());
                    rdbmsBuilder.getConstantFields().get().put(id, fields);
                    return fields.stream();
                }
            }
        } else {
            return getFields(builderContext, variable);
        }
    }

    private Stream<? extends RdbmsField> getFields(final RdbmsBuilderContext builderContext, final Variable variable) {
        final RdbmsBuilder<?> rdbmsBuilder = builderContext.getRdbmsBuilder();
        final Map<String, Object> queryParameters = builderContext.getQueryParameters();

        boolean isParameter = PARAMETER_VARIABLE_KEY.equals(variable.getCategory());
        final Object resolvedValue;
        if (isParameter) {
            resolvedValue = queryParameters != null ? queryParameters.get(variable.getName()) : null;
        } else {
            resolvedValue = rdbmsBuilder.getVariableResolver().resolve(Object.class, variable.getCategory(), variable.getName());
        }
        final Object parameterValue;
        if (AsmUtils.isEnumeration(variable.getType())) {
            if (resolvedValue != null) {
                EEnum eEnum = (EEnum) variable.getType();
                if (isParameter) {
                    parameterValue = eEnum.getEEnumLiteral((Integer) resolvedValue).getValue();
                } else {
                    parameterValue = eEnum.getEEnumLiteral(String.valueOf(resolvedValue)).getValue();
                }
            } else {
                parameterValue = null;
            }
        } else {
            parameterValue = rdbmsBuilder.getCoercer().coerce(resolvedValue, variable.getType().getInstanceClassName());
        }
        return getTargets(variable).map(t -> RdbmsNamedParameter.builder()
                .parameter(rdbmsBuilder.getParameterMapper().createParameter(parameterValue, variable.getType(), null))
                .index(rdbmsBuilder.getConstantCounter().getAndIncrement())
                .target(t.getTarget())
                .targetAttribute(t.getTargetAttribute())
                .alias(t.getAlias())
                .build());
    }
}
