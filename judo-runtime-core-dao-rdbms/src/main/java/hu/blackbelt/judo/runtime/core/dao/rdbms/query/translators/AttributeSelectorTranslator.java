package hu.blackbelt.judo.runtime.core.dao.rdbms.query.translators;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.AttributeSelector;
import hu.blackbelt.judo.meta.expression.DataExpression;
import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.ObjectExpression;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.expression.builder.jql.JqlExpressionBuilder;
import hu.blackbelt.judo.meta.expression.constant.Instance;
import hu.blackbelt.judo.meta.expression.object.ObjectVariableReference;
import hu.blackbelt.judo.meta.expression.variable.ObjectVariable;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.util.EcoreUtil;

import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@Builder
@Slf4j
public class AttributeSelectorTranslator implements Function<AttributeSelector, Expression> {

    @NonNull
    private final Function<Expression, Expression> translator;

    @NonNull
    private final AsmModelAdapter asmModelAdapter;

    @NonNull
    private final QueryFactory queryFactory;

    @NonNull
    private final AsmUtils asmUtils;

    @Override
    public Expression apply(final AttributeSelector attributeSelector) {
        final EClass mappedTransferObjectType = (EClass) attributeSelector.getObjectExpression().getObjectType(asmModelAdapter);
        final EAttribute transferAttribute = mappedTransferObjectType.getEAllAttributes().stream()
                .filter(a -> Objects.equals(a.getName(), attributeSelector.getAttributeName()))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Attribute not found: " + attributeSelector));

        final EAttribute entityAttribute = asmUtils.getMappedAttribute(transferAttribute)
                .orElseThrow(() -> new IllegalStateException("Attribute is not mapped: " + attributeSelector));

        if (entityAttribute.isDerived()) {
            final DataExpression expression = queryFactory.getEntityTypeExpressionsMap().get(entityAttribute.getEContainingClass())
                    .getGetterAttributeExpressions().get(entityAttribute);
            final DataExpression cloned = EcoreUtil.copy(expression);
            final Expression base = translator.apply(attributeSelector.getObjectExpression());
            final ObjectVariable objectVariable;
            if (base instanceof ObjectVariableReference) {
                objectVariable = ((ObjectVariableReference) base).getVariable();
            } else if (base instanceof ObjectVariable) {
                objectVariable = (ObjectVariable) base;
            } else {
                throw new IllegalStateException("Unsupported object variable");
            }

            collectSelfReferences(cloned).stream().forEach(selfReference -> selfReference.setVariable(objectVariable));

            return cloned;
        } else {
            final AttributeSelector selector = EcoreUtil.copy(attributeSelector);
            selector.setObjectExpression((ObjectExpression) translator.apply(attributeSelector.getObjectExpression()));
            selector.setAttributeName(entityAttribute.getName());
            return selector;
        }
    }

    private EList<ObjectVariableReference> collectSelfReferences(final Expression expression) {
        if (expression instanceof ObjectVariableReference) {
            final ObjectVariableReference variableReference = (ObjectVariableReference) expression;
            if (JqlExpressionBuilder.SELF_NAME.equals(variableReference.getVariableName()) && variableReference.getVariable() instanceof Instance) {
                return ECollections.singletonEList(variableReference);
            } else {
                return ECollections.emptyEList();
            }
        } else {
            return ECollections.asEList(expression.getOperands().stream().flatMap(o ->
                    collectSelfReferences(o).stream())
                    .collect(Collectors.toList()));
        }
    }
}
