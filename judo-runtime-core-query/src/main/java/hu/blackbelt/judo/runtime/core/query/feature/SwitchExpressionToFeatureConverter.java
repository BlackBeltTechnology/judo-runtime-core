package hu.blackbelt.judo.runtime.core.query.feature;

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

import hu.blackbelt.judo.meta.expression.SwitchCase;
import hu.blackbelt.judo.meta.expression.SwitchExpression;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import java.util.Iterator;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class SwitchExpressionToFeatureConverter extends ExpressionToFeatureConverter<SwitchExpression> {

    public SwitchExpressionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final SwitchExpression expression, final Context context, final FeatureTargetMapping targetMapping) {

        Feature feature = null;
        Function lastFeature = null;
        for (final Iterator<SwitchCase> it = expression.getCases().iterator(); it.hasNext(); ) {
            final SwitchCase switchCase = it.next();

            final Function function = newFunctionBuilder()
                    .withSignature(FunctionSignature.CASE_WHEN)
                    .withParameters(newFunctionParameterBuilder()
                            .withParameterName(ParameterName.CONDITION)
                            .withParameterValue(factory.convert(switchCase.getCondition(), context, null))
                            .build())
                    .withParameters(newFunctionParameterBuilder()
                            .withParameterName(ParameterName.LEFT)
                            .withParameterValue(factory.convert(switchCase.getExpression(), context, null))
                            .build())
                    .withConstraints(getConstraints(targetMapping != null ? targetMapping.getTargetAttribute() : null))
                    .build();
            context.addFeature(function);

            if (feature == null) {
                feature = function;
            } else {
                lastFeature.getParameters().add(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.RIGHT)
                        .withParameterValue(function)
                        .build());
            }
            if (!it.hasNext()) {
                if (expression.getDefaultExpression() != null) {
                    function.getParameters().add(newFunctionParameterBuilder()
                            .withParameterName(ParameterName.RIGHT)
                            .withParameterValue(factory.convert(expression.getDefaultExpression(), context, null))
                            .build());
                } else {
                    final Feature undefined = newFunctionBuilder().withSignature(FunctionSignature.UNDEFINED).build();
                    function.getParameters().add(newFunctionParameterBuilder()
                            .withParameterName(ParameterName.RIGHT)
                            .withParameterValue(undefined)
                            .build());
                    context.addFeature(undefined);
                }
            }
            lastFeature = function;
        }

        return feature;
    }
}
