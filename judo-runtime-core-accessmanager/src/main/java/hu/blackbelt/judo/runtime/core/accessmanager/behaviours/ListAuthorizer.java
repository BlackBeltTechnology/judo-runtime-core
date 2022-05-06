package hu.blackbelt.judo.runtime.core.accessmanager.behaviours;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.accessmanager.api.SignedIdentifier;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EOperation;

import java.util.Collection;
import java.util.Objects;

@Slf4j
@RequiredArgsConstructor
public class ListAuthorizer extends BehaviourAuthorizer {

    @NonNull
    private AsmUtils asmUtils;

    @Override
    public boolean isSuitableForOperation(final EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.LIST).isPresent();
    }

    @Override
    public void authorize(String actorFqName, Collection<String> publicActors, final SignedIdentifier signedIdentifier, final EOperation operation) {
        final ENamedElement owner = asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalStateException("No owner of operation found"));

        if (!AsmUtils.getExtensionAnnotationListByName(owner, "exposedBy").stream()
                .anyMatch(a -> publicActors.contains(a.getDetails().get("value")) || Objects.equals(actorFqName, a.getDetails().get("value")))) {
            throw new SecurityException("Permission denied");
        }
    }
}
