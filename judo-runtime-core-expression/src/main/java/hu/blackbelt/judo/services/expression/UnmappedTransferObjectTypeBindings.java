package hu.blackbelt.judo.services.expression;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.DataExpression;
import hu.blackbelt.judo.meta.expression.ReferenceExpression;
import lombok.NonNull;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@lombok.Getter
@lombok.Builder
public class UnmappedTransferObjectTypeBindings {

    @NonNull
    private final EClass unmappedTransferObjectType;

    @NonNull
    private final EMap<EAttribute, DataExpression> dataExpressions = ECollections.asEMap(new ConcurrentHashMap<>());

    @NonNull
    private final EMap<EReference, ReferenceExpression> navigationExpressions = ECollections.asEMap(new ConcurrentHashMap<>());

    @Override
    public String toString() {
        return "FROM: " + AsmUtils.getClassifierFQName(unmappedTransferObjectType)
                + (dataExpressions.isEmpty() ? "" : dataExpressions.stream().map(e -> "\n  - data expression " + e.getKey().getName() + ": " + e.getValue()).collect(Collectors.joining()))
                + (navigationExpressions.isEmpty() ? "" : navigationExpressions.stream().map(e -> "\n  - navigation expression " + e.getKey().getName() + ": " + e.getValue()).collect(Collectors.joining()));
    }
}
