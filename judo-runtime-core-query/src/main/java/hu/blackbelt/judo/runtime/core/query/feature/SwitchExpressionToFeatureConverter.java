package hu.blackbelt.judo.runtime.core.query.feature;

import hu.blackbelt.judo.meta.expression.SwitchCase;
import hu.blackbelt.judo.meta.expression.SwitchExpression;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;

import java.util.Iterator;

import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.newFunctionParameterBuilder;

public class SwitchExpressionToFeatureConverter extends ExpressionToFeatureConverter<SwitchExpression> {

    public SwitchExpressionToFeatureConverter(FeatureFactory factory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
    }

    @Override
    public Feature convert(final SwitchExpression expression, final Context context, final FeatureTargetMapping targetMapping) {

        Feature feature = null;
        Function lastFeature = null;
        for (final Iterator<SwitchCase> it = expression.getCases().iterator(); it.hasNext(); ) {
            final SwitchCase switchCase = it.next();

            final Function function = newFunctionBuilder()
                    .withSignature(FunctionSignature.CASE_WHEN)
                    .withParameters(newFunctionParameterBuilder()
                            .withParameterName(ParameterName.CONDITION)
                            .withParameterValue(factory.convert(switchCase.getCondition(), context, null))
                            .build())
                    .withParameters(newFunctionParameterBuilder()
                            .withParameterName(ParameterName.LEFT)
                            .withParameterValue(factory.convert(switchCase.getExpression(), context, null))
                            .build())
                    .withConstraints(getConstraints(targetMapping != null ? targetMapping.getTargetAttribute() : null))
                    .build();
            context.addFeature(function);

            if (feature == null) {
                feature = function;
            } else {
                lastFeature.getParameters().add(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.RIGHT)
                        .withParameterValue(function)
                        .build());
            }
            if (!it.hasNext()) {
                if (expression.getDefaultExpression() != null) {
                    function.getParameters().add(newFunctionParameterBuilder()
                            .withParameterName(ParameterName.RIGHT)
                            .withParameterValue(factory.convert(expression.getDefaultExpression(), context, null))
                            .build());
                } else {
                    final Feature undefined = newFunctionBuilder().withSignature(FunctionSignature.UNDEFINED).build();
                    function.getParameters().add(newFunctionParameterBuilder()
                            .withParameterName(ParameterName.RIGHT)
                            .withParameterValue(undefined)
                            .build());
                    context.addFeature(undefined);
                }
            }
            lastFeature = function;
        }

        return feature;
    }
}
