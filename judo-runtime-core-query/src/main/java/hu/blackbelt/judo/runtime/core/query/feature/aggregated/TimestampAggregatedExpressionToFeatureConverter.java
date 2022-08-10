package hu.blackbelt.judo.runtime.core.query.feature.aggregated;

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

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.temporal.TimestampAggregatedExpression;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;
import hu.blackbelt.judo.runtime.core.query.JoinFactory;
import hu.blackbelt.judo.runtime.core.query.feature.ExpressionToFeatureConverter;
import org.eclipse.emf.ecore.EClass;

import static hu.blackbelt.judo.meta.query.runtime.QueryUtils.getNextSubSelectAlias;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.*;

public class TimestampAggregatedExpressionToFeatureConverter extends ExpressionToFeatureConverter<TimestampAggregatedExpression> {

    private final JoinFactory joinFactory;

    public TimestampAggregatedExpressionToFeatureConverter(final FeatureFactory factory, final JoinFactory joinFactory, final AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
        this.joinFactory = joinFactory;
    }

    @Override
    public Feature convert(final TimestampAggregatedExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        final String iteratorVariableName = expression.getCollectionExpression().getIteratorVariableName();

        final EClass iteratorType = (EClass) expression.getCollectionExpression().getObjectType(modelAdapter);

        final SubSelect subSelect = newSubSelectBuilder()
                .withAlias(getNextSubSelectAlias(context.getSourceCounter()))
                .withSelect(newSelectBuilder()
                        .withAlias(getNextSubSelectAlias(context.getSourceCounter()))
                        .withFrom(iteratorType)
                        .withMainTarget(newTargetBuilder()
                                .withIndex(context.getTargetCounter().incrementAndGet())
                                .build())
                        .build())
                .build();
        joinFactory.convertNavigationToJoins(context, subSelect, expression.getCollectionExpression(), false);
        subSelect.setEmbeddedSelect(subSelect.getSelect());
        subSelect.getSelect().getTargets().add(subSelect.getSelect().getMainTarget());
        subSelect.getSelect().getMainTarget().setNode(subSelect.getSelect());

        final FunctionSignature signature;
        switch (expression.getOperator()) {
            case MIN: {
                signature = FunctionSignature.MIN_TIMESTAMP;
                break;
            }
            case MAX: {
                signature = FunctionSignature.MAX_TIMESTAMP;
                break;
            }
            case AVG: {
                signature = FunctionSignature.AVG_TIMESTAMP;
                break;
            }
            default: {
                throw new UnsupportedOperationException("Unsupported operation: " + expression.getOperator());
            }
        }

        final Context newContext = context.clone(iteratorVariableName, subSelect.getSelect());
        final Feature itemFeature = factory.convert(expression.getExpression(), newContext, null);
        final Function aggregatedFunction = newFunctionBuilder()
                .withSignature(signature)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.TIMESTAMP)
                        .withParameterValue(itemFeature)
                        .build())
                .withConstraints(getConstraints(targetMapping != null ? targetMapping.getTargetAttribute() : null))
                .build();

        newContext.addFeature(aggregatedFunction);
        //subSelect.getSelect().getFeatures().add(aggregatedFunction);
        aggregatedFunction.getTargetMappings().add(newFeatureTargetMappingBuilder()
                .withTarget(subSelect.getSelect().getMainTarget())
                .build());

        final Feature feature = newSubSelectFeatureBuilder()
                .withSubSelect(subSelect)
                .withFeature(aggregatedFunction)
                .build();

        if (context.getNode() != null) {
            context.getNode().getSubSelects().add(subSelect);
            context.addFeature(feature);
        } else {
            throw new IllegalStateException("Not supported yet");
        }
        return feature;
    }
}
