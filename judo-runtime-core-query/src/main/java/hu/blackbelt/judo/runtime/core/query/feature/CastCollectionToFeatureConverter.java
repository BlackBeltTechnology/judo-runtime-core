package hu.blackbelt.judo.runtime.core.query.feature;

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
