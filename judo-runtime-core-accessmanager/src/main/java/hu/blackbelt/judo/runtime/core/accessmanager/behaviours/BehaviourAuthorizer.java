package hu.blackbelt.judo.runtime.core.accessmanager.behaviours;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

import hu.blackbelt.judo.dao.api.ValidationResult;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
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

    void checkCRUDFlag(final AsmModel asmModel, final ENamedElement element, final CRUDFlag... flag) {
        AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());
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
