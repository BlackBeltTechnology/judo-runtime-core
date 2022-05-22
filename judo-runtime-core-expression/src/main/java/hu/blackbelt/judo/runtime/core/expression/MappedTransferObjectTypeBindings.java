package hu.blackbelt.judo.runtime.core.expression;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.DataExpression;
import hu.blackbelt.judo.meta.expression.LogicalExpression;
import hu.blackbelt.judo.meta.expression.ReferenceExpression;
import lombok.NonNull;
import lombok.Setter;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Expression tree node of a given (mapped) transfer object type. It is the base of logical queries.
 */
@lombok.Getter
@lombok.Builder
public class MappedTransferObjectTypeBindings {

    /**
     * Mapped entity type of {@link #entityType}.
     */
    @NonNull
    private final EClass entityType;

    /**
     * Transfer object type that the node belongs to.
     */
    @NonNull
    private final EClass transferObjectType;

    /**
     * Filter expression of mapped transfer object type.
     */
    @Setter
    private LogicalExpression filter;

    /**
     * Getter attributes of transfer attributes.
     */
    private final EMap<EAttribute, DataExpression> getterAttributeExpressions = ECollections.asEMap(new ConcurrentHashMap<>());

    /**
     * Setter expressions of transfer attributes.
     */
    private final EMap<EAttribute, DataExpression> setterAttributeExpressions = ECollections.asEMap(new ConcurrentHashMap<>());

    /**
     * Getter expressions of the transfer object relations.
     */
    private final EMap<EReference, ReferenceExpression> getterReferenceExpressions = ECollections.asEMap(new ConcurrentHashMap<>());

    /**
     * Setter expressions of the transfer object relations.
     */
    private final EMap<EReference, ReferenceExpression> setterReferenceExpressions = ECollections.asEMap(new ConcurrentHashMap<>());

    /**
     * References (both single and multiple) of the transfer object type.
     */
    private final EMap<EReference, MappedTransferObjectTypeBindings> references = ECollections.asEMap(new ConcurrentHashMap<>());

    @Override
    public String toString() {
        return "FROM: " + AsmUtils.getClassifierFQName(entityType) + "\n"
                + "TO: " + AsmUtils.getClassifierFQName(transferObjectType)
                + (getterAttributeExpressions.isEmpty() ? "" : getterAttributeExpressions.stream().map(e -> "\n  - getter attribute " + e.getKey().getName() + ": " + e.getValue()).collect(Collectors.joining()))
                + (setterAttributeExpressions.isEmpty() ? "" : setterAttributeExpressions.stream().map(e -> "\n  - setter attribute " + e.getKey().getName() + ": " + e.getValue()).collect(Collectors.joining()))
                + (getterReferenceExpressions.isEmpty() ? "" : getterReferenceExpressions.stream().map(e -> "\n  - getter relation " + e.getKey().getName() + ": " + e.getValue()).collect(Collectors.joining()))
                + (setterReferenceExpressions.isEmpty() ? "" : setterReferenceExpressions.stream().map(e -> "\n  - setter relation " + e.getKey().getName() + ": " + e.getValue()).collect(Collectors.joining()));
    }
}
