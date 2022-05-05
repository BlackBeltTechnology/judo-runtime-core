package hu.blackbelt.judo.services.query.feature.aggregated;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.string.ConcatenateCollection;
import hu.blackbelt.judo.meta.query.Feature;
import hu.blackbelt.judo.meta.query.FeatureTargetMapping;
import hu.blackbelt.judo.services.query.Context;
import hu.blackbelt.judo.services.query.FeatureFactory;
import hu.blackbelt.judo.services.query.feature.ExpressionToFeatureConverter;

public class ConcatenateCollectionToFeatureConverter extends ExpressionToFeatureConverter<ConcatenateCollection> {

    public ConcatenateCollectionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final ConcatenateCollection expression, final Context context, final FeatureTargetMapping targetMapping) {
        throw new UnsupportedOperationException("Not supported yet");
    }
}
