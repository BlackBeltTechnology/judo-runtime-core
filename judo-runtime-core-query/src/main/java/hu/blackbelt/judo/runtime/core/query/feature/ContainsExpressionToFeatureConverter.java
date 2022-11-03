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

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.logical.ContainsExpression;
import hu.blackbelt.judo.meta.expression.variable.ObjectVariable;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.query.*;
import org.eclipse.emf.ecore.EClass;

import static hu.blackbelt.judo.meta.expression.object.util.builder.ObjectBuilders.newObjectVariableReferenceBuilder;
import static hu.blackbelt.judo.meta.query.runtime.QueryUtils.getNextSubSelectAlias;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.*;

public class ContainsExpressionToFeatureConverter extends ExpressionToFeatureConverter<ContainsExpression> {

    private final JoinFactory joinFactory;

    public ContainsExpressionToFeatureConverter(FeatureFactory factory, JoinFactory joinFactory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
        this.joinFactory = joinFactory;
    }

    @Override
    public Feature convert(final ContainsExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        /*
         * e.g.: collectionOfA!contains(instanceOfB)
         * Concept: query 'collectionOfA' and filter for 'instanceOfB'
         *      For a filtering condition simple equality check is used in which the left operand is the instance argument itself (instanceOfB)
         *      and the right argument is a collection iterator created during feature conversion.
         */
        final EClass collectionClass = (EClass) expression.getCollectionExpression().getObjectType(modelAdapter);
        final SubSelect collectionSelect = newSubSelectBuilder()
                .withSelect(newSelectBuilder()
                                    .withFrom(collectionClass)
                                    .withAlias(getNextSubSelectAlias(context.getSourceCounter()))
                                    .withMainTarget(newTargetBuilder()
                                                            .withType(collectionClass)
                                                            .withIndex(context.getTargetCounter().incrementAndGet())
                                                            .build())
                                    .build())
                .withAlias(getNextSubSelectAlias(context.getSourceCounter()))
                .build();
        joinFactory.convertNavigationToJoins(context, collectionSelect, expression.getCollectionExpression(), false);
        collectionSelect.setEmbeddedSelect(collectionSelect.getSelect());
        collectionSelect.getSelect().getTargets().add(collectionSelect.getSelect().getMainTarget());
        collectionSelect.getSelect().getMainTarget().setContainerWithIdFeature(collectionSelect.getSelect(), false);

        final ObjectVariable collectionIterator = getCollectionIterator(expression.getCollectionExpression());

        final String iteratorName = expression.getCollectionExpression().getIteratorVariableName();

        final Filter containsFilter = newFilterBuilder()
                .withAlias(iteratorName)
                .build();
        collectionSelect.getSelect().getFilters().add(containsFilter);
        final Context filterContext = context.clone(iteratorName, containsFilter);
        Function containsCondition = newFunctionBuilder()
                .withSignature(FunctionSignature.EQUALS)
                .withParameters(newFunctionParameterBuilder()
                                        .withParameterName(ParameterName.LEFT)
                                        .withParameterValue(factory.convert(expression.getObjectExpression(), filterContext, null))
                                        .build())
                .withParameters(newFunctionParameterBuilder()
                                        .withParameterName(ParameterName.RIGHT)
                                        .withParameterValue(factory.convert(newObjectVariableReferenceBuilder()
                                                                                    .withVariable(collectionIterator)
                                                                                    .build(),
                                                                            filterContext, null))
                                        .build())
                .build();
        context.addFeature(containsCondition);
        containsFilter.setFeature(containsCondition);

        final Feature exists = newFunctionBuilder()
                .withSignature(FunctionSignature.EXISTS)
                .withParameters(newFunctionParameterBuilder()
                                        .withParameterName(ParameterName.COLLECTION)
                                        .withParameterValue(collectionSelect)
                                        .build())
                .build();

        if (context.getNode() != null) {
            context.getNode().getSubSelects().add(collectionSelect);
            context.addFeature(exists);
        } else {
            throw new IllegalStateException("Not supported yet");
        }

        return exists;
    }

}
