package hu.blackbelt.judo.runtime.core.query.feature;

import hu.blackbelt.judo.meta.expression.LogicalExpression;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.logical.ContainsExpression;
import hu.blackbelt.judo.meta.expression.operator.ObjectComparator;
import hu.blackbelt.judo.meta.expression.variable.ObjectVariable;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.FeatureFactory;
import hu.blackbelt.judo.runtime.core.query.JoinFactory;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.util.EcoreUtil;

import static hu.blackbelt.judo.meta.expression.logical.util.builder.LogicalBuilders.newObjectComparisonBuilder;
import static hu.blackbelt.judo.meta.expression.object.util.builder.ObjectBuilders.newObjectVariableReferenceBuilder;
import static hu.blackbelt.judo.meta.query.runtime.QueryUtils.getNextSubSelectAlias;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.*;

public class ContainsExpressionToFeatureConverter extends ExpressionToFeatureConverter<ContainsExpression> {

    private final JoinFactory joinFactory;

    public ContainsExpressionToFeatureConverter(FeatureFactory factory, JoinFactory joinFactory, AsmModelAdapter modelAdapter) {
        super(factory, modelAdapter);
        this.joinFactory = joinFactory;
    }

    @Override
    public Feature convert(final ContainsExpression expression, final Context context, final FeatureTargetMapping targetMapping) {
        final EClass subSelectClass = (EClass) expression.getCollectionExpression().getObjectType(modelAdapter);
        final SubSelect subSelect = newSubSelectBuilder()
                .withSelect(newSelectBuilder()
                        .withFrom(subSelectClass)
                        .withAlias(getNextSubSelectAlias(context.getSourceCounter()))
                        .withMainTarget(newTargetBuilder()
                                .withType(subSelectClass)
                                .withIndex(context.getTargetCounter().incrementAndGet())
                                .build())
                        .build())
                .withAlias(getNextSubSelectAlias(context.getSourceCounter()))
                .build();
        joinFactory.convertNavigationToJoins(context, subSelect, expression.getCollectionExpression(), false);
        subSelect.setEmbeddedSelect(subSelect.getSelect());
        subSelect.getSelect().getTargets().add(subSelect.getSelect().getMainTarget());
        subSelect.getSelect().getMainTarget().setContainerWithIdFeature(subSelect.getSelect(), false);

        final ObjectVariable iteratorVariable = getCollectionIterator(expression.getCollectionExpression());

        final LogicalExpression condition = newObjectComparisonBuilder()
                .withLeft(EcoreUtil.copy(expression.getObjectExpression()))
                .withOperator(ObjectComparator.EQUAL)
                .withRight(newObjectVariableReferenceBuilder()
                        .withVariable(iteratorVariable)
                        .build())
                .build();

        final String iteratorVariableName = expression.getCollectionExpression().getIteratorVariableName();

        final Filter filter = newFilterBuilder()
                .withAlias(iteratorVariableName)
                .build();
        subSelect.getSelect().getFilters().add(filter);
        final Context filterContext = context.clone(iteratorVariableName, filter);
        filter.setFeature(factory.convert(condition, filterContext, null));

        final Feature feature = newFunctionBuilder()
                .withSignature(FunctionSignature.EXISTS)
                .withParameters(newFunctionParameterBuilder()
                        .withParameterName(ParameterName.COLLECTION)
                        .withParameterValue(subSelect)
                        .build())
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
