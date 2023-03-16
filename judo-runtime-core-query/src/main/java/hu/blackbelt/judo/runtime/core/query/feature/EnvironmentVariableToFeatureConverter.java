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

import hu.blackbelt.judo.meta.expression.StringExpression;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.constant.StringConstant;
import hu.blackbelt.judo.meta.expression.variable.EnvironmentVariable;
import hu.blackbelt.judo.meta.expression.variable.MeasuredDecimalEnvironmentVariable;
import hu.blackbelt.judo.meta.measure.Unit;
import hu.blackbelt.judo.meta.query.Feature;
import hu.blackbelt.judo.meta.query.FeatureTargetMapping;
import hu.blackbelt.judo.meta.query.Variable;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;
import org.eclipse.emf.ecore.EDataType;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newVariableBuilder;

public class EnvironmentVariableToFeatureConverter extends ExpressionToFeatureConverter<EnvironmentVariable> {

    public EnvironmentVariableToFeatureConverter(final FeatureFactory factory, final AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final EnvironmentVariable environmentVariable, final Context context, final FeatureTargetMapping targetMapping) {
        final Optional<Unit> unit;
        if (environmentVariable instanceof MeasuredDecimalEnvironmentVariable) {
            unit = modelAdapter.getUnit((MeasuredDecimalEnvironmentVariable) environmentVariable);
        } else {
            unit = Optional.empty();
        }

        final StringExpression variableNameExpression = environmentVariable.getVariableName();
        checkArgument(variableNameExpression instanceof StringConstant, "Constant variable names are supported only by JQL");

        final Variable variable = newVariableBuilder()
                .withCategory(environmentVariable.getCategory())
                .withName(((StringConstant)variableNameExpression).getValue())
                .withType(modelAdapter.get(environmentVariable.getTypeName())
                        .filter(t -> t instanceof EDataType).map(t -> (EDataType) t)
                        .orElseThrow(() -> new IllegalStateException("Invalid type of (environment) variable")))
                .build();
        context.addFeature(variable);

        final Feature scaled;
        if (unit.isPresent()) {
            scaled = applyMeasureByUnit(context, variable, unit, targetMapping);
        } else {
            scaled = variable;
        }

        return scaled;
    }
}
