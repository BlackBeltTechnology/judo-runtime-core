package hu.blackbelt.judo.runtime.core.expression;

import hu.blackbelt.judo.meta.expression.DataExpression;
import hu.blackbelt.judo.meta.expression.ReferenceExpression;
import lombok.NonNull;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.util.concurrent.ConcurrentHashMap;

@lombok.Getter
@lombok.Builder
public class EntityTypeExpressions {

    @NonNull
    private final EClass entityType;

    private final EMap<EAttribute, DataExpression> getterAttributeExpressions = ECollections.asEMap(new ConcurrentHashMap<>());

    private final EMap<EReference, ReferenceExpression> getterReferenceExpressions = ECollections.asEMap(new ConcurrentHashMap<>());
}
