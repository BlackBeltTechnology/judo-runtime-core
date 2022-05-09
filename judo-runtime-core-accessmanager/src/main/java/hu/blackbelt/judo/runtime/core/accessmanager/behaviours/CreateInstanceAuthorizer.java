package hu.blackbelt.judo.runtime.core.accessmanager.behaviours;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.accessmanager.api.SignedIdentifier;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.ETypedElement;

import java.util.Collection;
import java.util.Objects;

@Slf4j
@RequiredArgsConstructor
@Builder
public class CreateInstanceAuthorizer extends BehaviourAuthorizer {

    @NonNull
    private AsmUtils asmUtils;

    @Override
    public boolean isSuitableForOperation(final EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.CREATE_INSTANCE || o == AsmUtils.OperationBehaviour.VALIDATE_CREATE).isPresent();
    }

    @Override
    public void authorize(String actorFqName, Collection<String> publicActors, final SignedIdentifier signedIdentifier, final EOperation operation) {
        final ENamedElement owner = asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalStateException("No owner of operation found"));

        checkCRUDFlag(asmUtils, owner, CRUDFlag.CREATE);
        if (!AsmUtils.getExtensionAnnotationListByName(owner, "exposedBy").stream()
                .anyMatch(a -> publicActors.contains(a.getDetails().get("value")) || Objects.equals(actorFqName, a.getDetails().get("value")))) {
            throw new SecurityException("Permission denied");
        }
        if (!AsmUtils.annotatedAsTrue(owner, "access")) {
            final ETypedElement producer = signedIdentifier.getProducedBy();
            if (producer == null) {
                throw new SecurityException("Unable to check permissions");
            }
            checkCRUDFlag(asmUtils, producer, CRUDFlag.UPDATE);
        }
    }
}
