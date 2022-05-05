package hu.blackbelt.judo.services.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.logical.UndefinedAttributeComparison;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.services.query.Context;
import hu.blackbelt.judo.services.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class UndefinedAttributeComparisonToFeatureConverter extends ExpressionToFeatureConverter<UndefinedAttributeComparison> {

    public UndefinedAttributeComparisonToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final UndefinedAttributeComparison expression, final Context context, final FeatureTargetMapping targetMapping) {
        final Feature feature = newFunctionBuilder()
                .withSignature(FunctionSignature.IS_UNDEFINED_ATTRIBUTE)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.ATTRIBUTE)
                        .withParameterValue(factory.convert(expression.getAttributeSelector(), context, null))
                        .build())
                .build();

        context.addFeature(feature);
        return feature;
    }
}
