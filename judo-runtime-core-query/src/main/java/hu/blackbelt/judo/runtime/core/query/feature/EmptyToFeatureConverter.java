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
import hu.blackbelt.judo.meta.expression.logical.Empty;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;
import hu.blackbelt.judo.runtime.core.query.JoinFactory;
import org.eclipse.emf.ecore.EClass;

import static hu.blackbelt.judo.meta.query.runtime.QueryUtils.getNextSubSelectAlias;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.*;

public class EmptyToFeatureConverter extends ExpressionToFeatureConverter<Empty> {

    private final JoinFactory joinFactory;

    public EmptyToFeatureConverter(FeatureFactory factory, final JoinFactory joinFactory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
        this.joinFactory = joinFactory;
    }

    @Override
    public Feature convert(final Empty expression, final Context context, final FeatureTargetMapping targetMapping) {
        final EClass subSelectClass = (EClass) expression.getCollectionExpression().getObjectType(modelAdapter);
        final Context subSelectContext = context.clone();
        final SubSelect subSelect = newSubSelectBuilder()
                .withSelect(newSelectBuilder()
                        .withFrom(subSelectClass)
                        .withAlias(getNextSubSelectAlias(context.getSourceCounter()))
                        .withMainTarget(newTargetBuilder()
                                .withType(subSelectClass)
                                .withIndex(context.getTargetCounter().incrementAndGet())
                                .build())
                        .build())
                .withAlias(getNextSubSelectAlias(context.getSourceCounter()))
                .build();
        subSelectContext.setNode(subSelect);
        joinFactory.convertNavigationToJoins(subSelectContext, subSelect, expression.getCollectionExpression(), false);
        subSelect.setEmbeddedSelect(subSelect.getSelect());
        subSelect.getSelect().getTargets().add(subSelect.getSelect().getMainTarget());
        subSelect.getSelect().getMainTarget().setContainerWithIdFeature(subSelect.getSelect(), false);

        final Feature feature = newFunctionBuilder()
                .withSignature(FunctionSignature.NOT_EXISTS)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.COLLECTION)
                        .withParameterValue(subSelect)
                        .build())
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
