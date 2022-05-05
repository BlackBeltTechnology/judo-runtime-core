package hu.blackbelt.judo.services.accessmanager.behaviours;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.services.accessmanager.api.SignedIdentifier;
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
public class GetInputRangeAuthorizer extends BehaviourAuthorizer {

    @NonNull
    private AsmUtils asmUtils;

    @Override
    public boolean isSuitableForOperation(final EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.GET_INPUT_RANGE).isPresent();
    }

    @Override
    public void authorize(String actorFqName, Collection<String> publicActors, final SignedIdentifier signedIdentifier, final EOperation operation) {
        if (signedIdentifier != null) {
            final ENamedElement owner = asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                    .orElseThrow(() -> new IllegalStateException("No owner of operation found"));

            if (!AsmUtils.getExtensionAnnotationListByName(owner, "exposedBy").stream()
                    .anyMatch(a -> publicActors.contains(a.getDetails().get("value")) || Objects.equals(actorFqName, a.getDetails().get("value")))) {
                throw new SecurityException("Permission denied");
            }
        }
    }
}
