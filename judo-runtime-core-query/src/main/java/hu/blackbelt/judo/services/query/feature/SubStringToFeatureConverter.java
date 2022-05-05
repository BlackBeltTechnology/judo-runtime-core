package hu.blackbelt.judo.services.query.feature;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.string.SubString;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.services.query.Context;
import hu.blackbelt.judo.services.query.FeatureFactory;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class SubStringToFeatureConverter extends ExpressionToFeatureConverter<SubString> {

    public SubStringToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final SubString expression, final Context context, final FeatureTargetMapping targetMapping) {
        final Feature feature = newFunctionBuilder()
                .withSignature(FunctionSignature.SUBSTRING_STRING)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.STRING)
                        .withParameterValue(factory.convert(expression.getExpression(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.POSITION)
                        .withParameterValue(factory.convert(expression.getPosition(), context, null))
                        .build())
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.LENGTH)
                        .withParameterValue(factory.convert(expression.getLength(), context, null))
                        .build())
                .withConstraints(getConstraints(targetMapping != null ? targetMapping.getTargetAttribute() : null))
                .build();

        context.addFeature(feature);
        return feature;
    }
}
