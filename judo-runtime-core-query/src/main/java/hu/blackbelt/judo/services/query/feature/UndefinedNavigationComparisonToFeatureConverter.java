package hu.blackbelt.judo.services.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.logical.UndefinedNavigationComparison;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.services.query.Context;
import hu.blackbelt.judo.services.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class UndefinedNavigationComparisonToFeatureConverter extends ExpressionToFeatureConverter<UndefinedNavigationComparison> {

    public UndefinedNavigationComparisonToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final UndefinedNavigationComparison expression, final Context context, final FeatureTargetMapping targetMapping) {
        final Feature feature = newFunctionBuilder()
                .withSignature(FunctionSignature.IS_UNDEFINED_OBJECT)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.RELATION)
                        .withParameterValue(factory.convert(expression.getObjectNavigationExpression(), context, null))
                        .build())
                .build();

        context.addFeature(feature);
        return feature;
    }
}
