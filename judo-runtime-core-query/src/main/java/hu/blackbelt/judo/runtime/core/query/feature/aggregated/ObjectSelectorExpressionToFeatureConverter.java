package hu.blackbelt.judo.runtime.core.query.feature.aggregated;

import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.object.ObjectSelectorExpression;
import hu.blackbelt.judo.meta.expression.object.ObjectVariableReference;
import hu.blackbelt.judo.meta.expression.variable.ObjectVariable;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;
import hu.blackbelt.judo.runtime.core.query.JoinFactory;
import hu.blackbelt.judo.runtime.core.query.feature.ExpressionToFeatureConverter;
import org.eclipse.emf.ecore.EClass;

import static hu.blackbelt.judo.meta.expression.object.util.builder.ObjectBuilders.newObjectVariableReferenceBuilder;
import static hu.blackbelt.judo.meta.query.runtime.QueryUtils.getNextSubSelectAlias;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.*;

public class ObjectSelectorExpressionToFeatureConverter extends ExpressionToFeatureConverter<ObjectSelectorExpression> {

    private final JoinFactory joinFactory;

    public ObjectSelectorExpressionToFeatureConverter(FeatureFactory factory, JoinFactory joinFactory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
        this.joinFactory = joinFactory;
    }

    @Override
    public Feature convert(final ObjectSelectorExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        final ObjectVariable iteratorVariable = getCollectionIterator(expression.getCollectionExpression());
        final String iteratorVariableName = iteratorVariable.getName();

        final EClass iteratorType = (EClass) expression.getCollectionExpression().getObjectType(modelAdapter);

        final Context subSelectContext = context.clone();
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
        subSelectContext.setNode(subSelect);
        joinFactory.convertNavigationToJoins(subSelectContext, subSelect, expression.getCollectionExpression(), true);
        subSelect.setEmbeddedSelect(subSelect.getSelect());
        subSelect.getSelect().getTargets().add(subSelect.getSelect().getMainTarget());
        subSelect.getSelect().getMainTarget().setNode(subSelect.getSelect());

        final ObjectVariableReference item = newObjectVariableReferenceBuilder().withVariable(iteratorVariable).build();
        final Context newContext = context.clone(iteratorVariableName, subSelect.getSelect());
        final ParameterType itemParameter = factory.convert(item, newContext, null);

        final Function selector;
        switch (expression.getOperator()) {
            case ANY: {
                selector = newFunctionBuilder()
                        .withSignature(FunctionSignature.MIN_INTEGER)
                        .withParameters(newFunctionParameterBuilder()
                                .withParameterName(ParameterName.NUMBER)
                                .withParameterValue(itemParameter)
                                .build())
                        .withConstraints(getConstraints(targetMapping != null ? targetMapping.getTargetAttribute() : null))
                        .build();
                break;
            }
            default:
                throw new IllegalStateException("Unsupported object selector");
        }

        newContext.addFeature(selector);
        selector.getTargetMappings().add(newFeatureTargetMappingBuilder()
                .withTarget(subSelect.getSelect().getMainTarget())
                .build());

        final Feature feature = newSubSelectFeatureBuilder()
                .withSubSelect(subSelect)
                .withFeature(selector)
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
