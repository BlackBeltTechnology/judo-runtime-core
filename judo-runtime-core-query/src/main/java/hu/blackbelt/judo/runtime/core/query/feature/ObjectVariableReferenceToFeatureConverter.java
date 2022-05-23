package hu.blackbelt.judo.runtime.core.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.object.ObjectVariableReference;
import hu.blackbelt.judo.meta.query.Feature;
import hu.blackbelt.judo.meta.query.FeatureTargetMapping;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newIdAttributeBuilder;

public class ObjectVariableReferenceToFeatureConverter extends ExpressionToFeatureConverter<ObjectVariableReference> {

    public ObjectVariableReferenceToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final ObjectVariableReference expression, final Context context, final FeatureTargetMapping targetMapping) {
        final Feature feature = newIdAttributeBuilder()
                .withNode(getSourceByVariableName(expression, context))
                .build();

        context.addFeature(feature);
        return feature;
    }
}
