package hu.blackbelt.judo.services.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.numeric.ExtractTimeExpression;
import hu.blackbelt.judo.meta.query.Feature;
import hu.blackbelt.judo.meta.query.FeatureTargetMapping;
import hu.blackbelt.judo.meta.query.FunctionSignature;
import hu.blackbelt.judo.meta.query.ParameterName;
import hu.blackbelt.judo.services.query.Context;
import hu.blackbelt.judo.services.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class ExtractTimeExpressionToFeatureConverter extends ExpressionToFeatureConverter<ExtractTimeExpression> {

    public ExtractTimeExpressionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final ExtractTimeExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        final FunctionSignature signature;
        switch (expression.getPart()) {
            case HOUR: {
                signature = FunctionSignature.HOURS_OF_TIME;
                break;
            }
            case MINUTE: {
                signature = FunctionSignature.MINUTES_OF_TIME;
                break;
            }
            case SECOND: {
                signature = FunctionSignature.SECONDS_OF_TIME;
                break;
            }
            case MILLISECOND: {
                signature = FunctionSignature.MILLISECONDS_OF_TIME;
                break;
            }
            default:
                throw new UnsupportedOperationException("Unsupported time part");
        }

        final Feature feature = newFunctionBuilder()
                .withSignature(signature)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.TIME)
                        .withParameterValue(factory.convert(expression.getTime(), context, null))
                        .build())
                .build();

        context.addFeature(feature);
        return feature;
    }
}
