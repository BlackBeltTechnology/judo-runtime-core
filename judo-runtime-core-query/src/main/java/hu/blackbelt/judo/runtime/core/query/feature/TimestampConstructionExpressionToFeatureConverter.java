package hu.blackbelt.judo.runtime.core.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.temporal.TimestampConstructionExpression;
import hu.blackbelt.judo.meta.query.Feature;
import hu.blackbelt.judo.meta.query.FeatureTargetMapping;
import hu.blackbelt.judo.meta.query.FunctionSignature;
import hu.blackbelt.judo.meta.query.ParameterName;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class TimestampConstructionExpressionToFeatureConverter extends ExpressionToFeatureConverter<TimestampConstructionExpression> {

    public TimestampConstructionExpressionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final TimestampConstructionExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        final Feature feature = newFunctionBuilder()
                .withSignature(FunctionSignature.TO_TIMESTAMP)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.YEAR)
                        .withParameterValue(factory.convert(expression.getYear(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.MONTH)
                        .withParameterValue(factory.convert(expression.getMonth(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.DAY)
                        .withParameterValue(factory.convert(expression.getDay(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.HOUR)
                        .withParameterValue(factory.convert(expression.getHour(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.MINUTE)
                        .withParameterValue(factory.convert(expression.getMinute(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.SECOND)
                        .withParameterValue(factory.convert(expression.getSecond(), context, null))
                        .build())
                .build();

        context.addFeature(feature);
        return feature;
    }
}
