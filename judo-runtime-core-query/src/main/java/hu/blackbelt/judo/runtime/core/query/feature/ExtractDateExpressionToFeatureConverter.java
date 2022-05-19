package hu.blackbelt.judo.runtime.core.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.numeric.ExtractDateExpression;
import hu.blackbelt.judo.meta.query.Feature;
import hu.blackbelt.judo.meta.query.FeatureTargetMapping;
import hu.blackbelt.judo.meta.query.FunctionSignature;
import hu.blackbelt.judo.meta.query.ParameterName;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class ExtractDateExpressionToFeatureConverter extends ExpressionToFeatureConverter<ExtractDateExpression> {

    public ExtractDateExpressionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final ExtractDateExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        final FunctionSignature signature;
        switch (expression.getPart()) {
            case YEAR: {
                signature = FunctionSignature.YEARS_OF_DATE;
                break;
            }
            case MONTH: {
                signature = FunctionSignature.MONTHS_OF_DATE;
                break;
            }
            case DAY: {
                signature = FunctionSignature.DAYS_OF_DATE;
                break;
            }
            default:
                throw new UnsupportedOperationException("Unsupported date part");
        }

        final Feature feature = newFunctionBuilder()
                .withSignature(signature)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.DATE)
                        .withParameterValue(factory.convert(expression.getDate(), context, null))
                        .build())
                .build();

        context.addFeature(feature);
        return feature;
    }
}
