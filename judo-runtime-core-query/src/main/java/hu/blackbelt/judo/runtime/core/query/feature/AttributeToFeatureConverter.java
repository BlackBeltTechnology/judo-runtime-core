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

import hu.blackbelt.judo.meta.expression.AttributeSelector;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.numeric.DecimalAttribute;
import hu.blackbelt.judo.meta.expression.numeric.IntegerAttribute;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;
import hu.blackbelt.judo.runtime.core.query.JoinFactory;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;

import java.util.Objects;
import java.util.Optional;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newAttributeBuilder;

public class AttributeToFeatureConverter extends ExpressionToFeatureConverter<AttributeSelector> {

    private final JoinFactory joinFactory;
    private final AsmModelAdapter modelAdapter;

    public AttributeToFeatureConverter(final FeatureFactory factory, final JoinFactory joinFactory, final AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
        this.joinFactory = joinFactory;
        this.modelAdapter = modelAdapter;
    }

    @Override
    public Feature convert(final AttributeSelector attributeSelector, final Context context, final FeatureTargetMapping targetMapping) {
        final String attributeName = attributeSelector.getAttributeName();
        final EClass sourceType = (EClass) attributeSelector.getObjectExpression().getObjectType(modelAdapter);
        final Optional<EAttribute> entityAttribute = sourceType.getEAllAttributes().stream().filter(a -> Objects.equals(a.getName(), attributeName)).findAny();

        // create additional JOINs for navigation, ie. derived attribute via multiple navigation steps
        final JoinFactory.PathEnds pathEnds = joinFactory.convertNavigationToJoins(context, context.getNode(), attributeSelector.getObjectExpression(), false);

        final boolean measured;
        if (attributeSelector instanceof IntegerAttribute) {
            measured = modelAdapter.isMeasured((IntegerAttribute) attributeSelector);
        } else if (attributeSelector instanceof DecimalAttribute) {
            measured = modelAdapter.isMeasured((DecimalAttribute) attributeSelector);
        } else {
            measured = false;
        }

        // create new attribute
        final Attribute attribute = newAttributeBuilder()
                .withNode(pathEnds.getPartner())
                .withSourceAttribute((EAttribute) sourceType.getEStructuralFeature(attributeName))
                .build();
        context.addFeature(attribute);

        if (attribute.getSourceAttribute().isDerived()) {
            throw new IllegalStateException(String.format("Derived attributes must be resolved by expression builder: %s (%s)",
                                                          attribute.getSourceAttribute().getName(),
                                                          modelAdapter.getFqName(attribute.getSourceAttribute().eContainer())));
        }

        final Feature scaled;
        if (measured) {
            scaled = applyMeasure(context, attribute, entityAttribute.orElse(null), targetMapping);
        } else {
            scaled = attribute;
        }

        return scaled;
    }
}
