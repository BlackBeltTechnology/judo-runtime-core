package hu.blackbelt.judo.runtime.core.accessmanager.behaviours;

import hu.blackbelt.judo.dao.api.ValidationResult;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.exception.AccessDeniedException;
import hu.blackbelt.judo.runtime.core.accessmanager.api.SignedIdentifier;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAnnotation;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;

import java.util.*;

@Slf4j
public abstract class BehaviourAuthorizer {

    public abstract boolean isSuitableForOperation(EOperation operation);

    public abstract void authorize(String actorFqName, Collection<String> publicActors, SignedIdentifier signedIdentifier, EOperation operation);

    void checkCRUDFlag(final AsmUtils asmUtils, final ENamedElement element, final CRUDFlag... flag) {
        final Optional<EAnnotation> permissions = AsmUtils.getExtensionAnnotationByName(element, "permissions", false);

        if (log.isTraceEnabled()) {
            log.trace("Checking {} flag on {} ...", Arrays.asList(flag), element.getName());
        }

        if (permissions.isEmpty() || Arrays.stream(flag)
                .noneMatch(f -> Boolean.parseBoolean(permissions.get().getDetails().get(f.permissionName)))) {
            final Optional<? extends ENamedElement> operationOwner = element instanceof EOperation ? asmUtils.getOwnerOfOperationWithDefaultBehaviour((EOperation) element) : Optional.empty();
            final Map<String, Object> details = new LinkedHashMap<>();
            details.put("MISSING_PRIVILEGES", Arrays.asList(flag));
            if (element instanceof EOperation && operationOwner.isPresent() && !AsmUtils.OperationBehaviour.GET_REFERENCE_RANGE.equals(AsmUtils.getBehaviour((EOperation) element).orElse(null))) {
                final EAnnotation operationOwnerPermissions = AsmUtils.getExtensionAnnotationByName(operationOwner.get(), "permissions", false)
                        .orElseThrow(() -> new IllegalStateException("Permission denied, no permission annotation found on operation owner"));

                if (Arrays.stream(flag).noneMatch(f -> Boolean.parseBoolean(operationOwnerPermissions.getDetails().get(f.permissionName)))) {
                    log.info("Operation failed, permission denied (by CRUD operation)");
                    details.put("MODEL_ELEMENT", AsmUtils.getOperationFQName((EOperation) element));
                    throw new AccessDeniedException(ValidationResult.builder()
                            .code("PERMISSION_DENIED")
                            .level(ValidationResult.Level.ERROR)
                            .location(element.getName())
                            .details(details)
                            .build());
                }
            } else {
                log.info("Operation failed, permission denied (by reference)");
                if (element instanceof EReference) {
                    details.put("MODEL_ELEMENT", AsmUtils.getReferenceFQName((EReference) element));
                } else if (element instanceof EOperation) {
                    details.put("MODEL_ELEMENT", AsmUtils.getOperationFQName((EOperation) element));
                } else {
                    details.put("MODEL_ELEMENT", element.getName());
                }
                throw new AccessDeniedException(ValidationResult.builder()
                        .code("PERMISSION_DENIED")
                        .level(ValidationResult.Level.ERROR)
                        .location(element.getName())
                        .details(details)
                        .build());
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Expected CRUD flag {} found on {}", Arrays.asList(flag), element.getName());
        }
    }

    @RequiredArgsConstructor
    public enum CRUDFlag {
        CREATE("create"), UPDATE("update"), DELETE("delete");

        @NonNull
        private final String permissionName;
    }
}
