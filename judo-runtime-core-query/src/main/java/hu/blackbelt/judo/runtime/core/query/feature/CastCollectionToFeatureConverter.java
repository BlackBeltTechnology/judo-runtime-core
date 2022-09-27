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
import hu.blackbelt.judo.meta.expression.collection.CastCollection;
import hu.blackbelt.judo.meta.query.Feature;
import hu.blackbelt.judo.meta.query.FeatureTargetMapping;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;
import hu.blackbelt.judo.runtime.core.query.JoinFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newIdAttributeBuilder;

public class CastCollectionToFeatureConverter extends ExpressionToFeatureConverter<CastCollection> {

    private final JoinFactory joinFactory;

    public CastCollectionToFeatureConverter(FeatureFactory factory, JoinFactory joinFactory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
        this.joinFactory = joinFactory;
    }

    @Override
    public Feature convert(final CastCollection expression, final Context context, final FeatureTargetMapping targetMapping) {
        // create additional JOINs for navigation, ie. derived attribute via multiple navigation steps
        final JoinFactory.PathEnds pathEnds = joinFactory.convertNavigationToJoins(context, context.getNode(), expression, false);

        final Feature feature = newIdAttributeBuilder()
                .withNode(pathEnds.getPartner())
                .build();

        context.addFeature(feature);
        return feature;
    }
}
