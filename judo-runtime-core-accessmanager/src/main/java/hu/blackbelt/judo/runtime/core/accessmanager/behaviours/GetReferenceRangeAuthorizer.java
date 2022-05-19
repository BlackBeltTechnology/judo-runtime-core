package hu.blackbelt.judo.runtime.core.accessmanager.behaviours;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.accessmanager.api.SignedIdentifier;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.ETypedElement;

import java.util.Collection;

@RequiredArgsConstructor
@Builder
public class GetReferenceRangeAuthorizer extends BehaviourAuthorizer {

    @NonNull
    private AsmUtils asmUtils;

    @Override
    public boolean isSuitableForOperation(final EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.GET_REFERENCE_RANGE).isPresent();
    }

    @Override
    public void authorize(String actorFqName, Collection<String> publicActors, final SignedIdentifier signedIdentifier, final EOperation operation) {
        if (signedIdentifier != null) {
            final ETypedElement producer = signedIdentifier.getProducedBy();
            if (producer == null) {
                throw new SecurityException("Unable to check permissions");
            }
            checkCRUDFlag(asmUtils, producer, CRUDFlag.CREATE, CRUDFlag.UPDATE);
        }
    }
}
