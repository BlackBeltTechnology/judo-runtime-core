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

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.accessmanager.api.SignedIdentifier;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.ETypedElement;

import java.util.Collection;
import java.util.Objects;

@RequiredArgsConstructor
@Builder
public class CreateInstanceAuthorizer extends BehaviourAuthorizer {

    @NonNull
    private AsmModel asmModel;

    @Override
    public boolean isSuitableForOperation(final EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.CREATE_INSTANCE || o == AsmUtils.OperationBehaviour.VALIDATE_CREATE).isPresent();
    }

    @Override
    public void authorize(String actorFqName, Collection<String> publicActors, final SignedIdentifier signedIdentifier, final EOperation operation) {
        AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());
        final ENamedElement owner = asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalStateException("No owner of operation found"));

        checkCRUDFlag(asmModel, owner, CRUDFlag.CREATE);
        if (AsmUtils.getExtensionAnnotationListByName(owner, "exposedBy").stream()
                .noneMatch(a -> publicActors.contains(a.getDetails().get("value")) || Objects.equals(actorFqName, a.getDetails().get("value")))) {
            throw new SecurityException("Permission denied");
        }
        if (!AsmUtils.annotatedAsTrue(owner, "access")) {
            final ETypedElement producer = signedIdentifier.getProducedBy();
            if (producer == null) {
                throw new SecurityException("Unable to check permissions");
            }
            checkCRUDFlag(asmModel, producer, CRUDFlag.UPDATE);
        }
    }
}
