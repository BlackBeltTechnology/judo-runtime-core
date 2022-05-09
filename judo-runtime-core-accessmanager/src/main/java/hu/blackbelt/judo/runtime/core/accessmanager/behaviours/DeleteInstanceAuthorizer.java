package hu.blackbelt.judo.runtime.core.accessmanager.behaviours;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.accessmanager.api.SignedIdentifier;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.ETypedElement;

import java.util.Collection;

@Slf4j
@RequiredArgsConstructor
@Builder
public class DeleteInstanceAuthorizer extends BehaviourAuthorizer {

    @NonNull
    private AsmUtils asmUtils;

    @Override
    public boolean isSuitableForOperation(final EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.DELETE_INSTANCE).isPresent();
    }

    @Override
    public void authorize(String actorFqName, Collection<String> publicActors, final SignedIdentifier signedIdentifier, final EOperation operation) {
        final ETypedElement producer = signedIdentifier.getProducedBy();
        if (producer == null) {
            throw new SecurityException("Unable to check permissions");
        }
        checkCRUDFlag(asmUtils, producer, CRUDFlag.DELETE);
    }
}
