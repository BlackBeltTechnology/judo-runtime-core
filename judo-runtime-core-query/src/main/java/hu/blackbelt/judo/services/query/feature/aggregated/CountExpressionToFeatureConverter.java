package hu.blackbelt.judo.services.query.feature.aggregated;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.numeric.CountExpression;
import hu.blackbelt.judo.meta.expression.object.ObjectVariableReference;
import hu.blackbelt.judo.meta.expression.variable.ObjectVariable;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.services.query.Context;
import hu.blackbelt.judo.services.query.FeatureFactory;
import hu.blackbelt.judo.services.query.JoinFactory;
import hu.blackbelt.judo.services.query.feature.ExpressionToFeatureConverter;
import org.eclipse.emf.ecore.EClass;

import static hu.blackbelt.judo.meta.expression.object.util.builder.ObjectBuilders.newObjectVariableReferenceBuilder;
import static hu.blackbelt.judo.meta.query.runtime.QueryUtils.getNextSubSelectAlias;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.*;

public class CountExpressionToFeatureConverter extends ExpressionToFeatureConverter<CountExpression> {

    private final JoinFactory joinFactory;

    public CountExpressionToFeatureConverter(FeatureFactory factory, JoinFactory joinFactory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
        this.joinFactory = joinFactory;
    }

    @Override
    public Feature convert(final CountExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        final ObjectVariable iteratorVariable = getCollectionIterator(expression.getCollectionExpression());
        final String iteratorVariableName = iteratorVariable.getName();

        final EClass iteratorType = (EClass) expression.getCollectionExpression().getObjectType(modelAdapter);

        final SubSelect subSelect = newSubSelectBuilder()
                .withAlias(getNextSubSelectAlias(context.getSourceCounter()))
                .withSelect(newSelectBuilder()
                        .withAlias(getNextSubSelectAlias(context.getSourceCounter()))
                        .withFrom(iteratorType)
                        .withMainTarget(newTargetBuilder()
                                .withIndex(context.getTargetCounter().incrementAndGet())
                                .build())
                        .build())
                .build();

        joinFactory.convertNavigationToJoins(context, subSelect, expression.getCollectionExpression(), false);
        subSelect.setEmbeddedSelect(subSelect.getSelect());
        subSelect.getSelect().getTargets().add(subSelect.getSelect().getMainTarget());
        subSelect.getSelect().getMainTarget().setNode(subSelect.getSelect());

        final ObjectVariableReference item = newObjectVariableReferenceBuilder().withVariable(iteratorVariable).build();
        final Context newContext = context.clone(iteratorVariableName, subSelect.getSelect());
        final ParameterType itemParameter = factory.convert(item, newContext, null);
        final Function count = newFunctionBuilder()
                .withSignature(FunctionSignature.COUNT)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.ITEM)
                        .withParameterValue(itemParameter)
                        .build())
                .withConstraints(getConstraints(targetMapping != null ? targetMapping.getTargetAttribute() : null))
                .build();

        newContext.addFeature(count);
        //subSelect.getSelect().getFeatures().add(count);
        count.getTargetMappings().add(newFeatureTargetMappingBuilder()
                .withTarget(subSelect.getSelect().getMainTarget())
                .build());

        final Feature feature = newSubSelectFeatureBuilder()
                .withSubSelect(subSelect)
                .withFeature(count)
                .build();

        if (context.getNode() != null) {
            context.getNode().getSubSelects().add(subSelect);
            context.addFeature(feature);
        } else {
            throw new IllegalStateException("Not supported yet");
        }
        return feature;
    }
}
