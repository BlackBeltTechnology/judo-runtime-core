package hu.blackbelt.judo.services.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.numeric.ExtractTimestampExpression;
import hu.blackbelt.judo.meta.query.Feature;
import hu.blackbelt.judo.meta.query.FeatureTargetMapping;
import hu.blackbelt.judo.meta.query.FunctionSignature;
import hu.blackbelt.judo.meta.query.ParameterName;
import hu.blackbelt.judo.services.query.Context;
import hu.blackbelt.judo.services.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class ExtractTimestampExpressionToFeatureConverter extends ExpressionToFeatureConverter<ExtractTimestampExpression> {

    public ExtractTimestampExpressionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final ExtractTimestampExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        final FunctionSignature signature;
        switch (expression.getPart()) {
            case YEAR: {
                signature = FunctionSignature.YEARS_OF_TIMESTAMP;
                break;
            }
            case MONTH: {
                signature = FunctionSignature.MONTHS_OF_TIMESTAMP;
                break;
            }
            case DAY: {
                signature = FunctionSignature.DAYS_OF_TIMESTAMP;
                break;
            }
            case HOUR: {
                signature = FunctionSignature.HOURS_OF_TIMESTAMP;
                break;
            }
            case MINUTE: {
                signature = FunctionSignature.MINUTES_OF_TIMESTAMP;
                break;
            }
            case SECOND: {
                signature = FunctionSignature.SECONDS_OF_TIMESTAMP;
                break;
            }
            case MILLISECOND: {
                signature = FunctionSignature.MILLISECONDS_OF_TIMESTAMP;
                break;
            }
            default:
                throw new UnsupportedOperationException("Unsupported timestamp part");
        }

        final Feature feature = newFunctionBuilder()
                .withSignature(signature)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.TIMESTAMP)
                        .withParameterValue(factory.convert(expression.getTimestamp(), context, null))
                        .build())
                .build();

        context.addFeature(feature);
        return feature;
    }
}
