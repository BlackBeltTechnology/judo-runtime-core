package hu.blackbelt.judo.runtime.core.accessmanager;

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
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AccessManager;
import hu.blackbelt.judo.runtime.core.accessmanager.api.SignedIdentifier;
import hu.blackbelt.judo.runtime.core.accessmanager.behaviours.*;
import hu.blackbelt.judo.runtime.core.exception.AccessDeniedException;
import hu.blackbelt.judo.runtime.core.exception.AuthenticationRequiredException;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EOperation;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class DefaultAccessManager implements AccessManager {

    @NonNull
    AsmModel asmModel;

    private final Collection<String> publicActors = new HashSet<>();

    private Collection<BehaviourAuthorizer> authorizers;

    private void setupAuthorizers(final AsmUtils asmUtils) {
        authorizers = Arrays.asList(
                new ListAuthorizer(asmUtils),
                new CreateInstanceAuthorizer(asmUtils),
                new UpdateInstanceAuthorizer(asmUtils),
                new DeleteInstanceAuthorizer(asmUtils),
                new RefreshAuthorizer(asmUtils),
                new SetReferenceAuthorizer(asmUtils),
                new UnsetReferenceAuthorizer(asmUtils),
                new AddReferenceAuthorizer(asmUtils),
                new RemoveReferenceAuthorizer(asmUtils),
                new GetReferenceRangeAuthorizer(asmUtils),
                new GetInputRangeAuthorizer(asmUtils),
                new GetTemplateAuthorizer(asmUtils)
        );
    }

    @Builder
    public DefaultAccessManager(@NonNull AsmModel asmModel) {
        this.asmModel = asmModel;
        AsmUtils asmUtils = new AsmUtils(asmModel.getResourceSet());
        setupAuthorizers(asmUtils);

        publicActors.addAll(asmUtils.all(EClass.class)
                .filter(c -> AsmUtils.isActorType(c) &&
                        AsmUtils.getExtensionAnnotationByName(c, "realm", false)
                                .map(a -> a.getDetails().get("value") == null || a.getDetails().get("value").isEmpty())
                                .orElse(true))
                .map(AsmUtils::getClassifierFQName)
                .collect(Collectors.toSet()));
    }

    @Override
    public void authorizeOperation(final EOperation operation, final SignedIdentifier signedIdentifier, final Map<String, Object> exchange) {
        if (exchange.get(Dispatcher.PRINCIPAL_KEY) != null && !(exchange.get(Dispatcher.PRINCIPAL_KEY) instanceof JudoPrincipal)) {
            throw new IllegalStateException("Unsupported principal");
        }

        final JudoPrincipal principal = exchange.get(Dispatcher.PRINCIPAL_KEY) != null ? (JudoPrincipal) exchange.get(Dispatcher.PRINCIPAL_KEY) : null;
        final String actorFqName = principal != null ? principal.getClient() : null;

        final boolean exposedForPublicOrTokenActor = AsmUtils.getExtensionAnnotationListByName(operation, "exposedBy").stream()
                .anyMatch(a -> publicActors.contains(a.getDetails().get("value")) || Objects.equals(actorFqName, a.getDetails().get("value")));
        final boolean metadataOperation = AsmUtils.OperationBehaviour.GET_METADATA.equals(AsmUtils.getBehaviour(operation).orElse(null));

        if (!exposedForPublicOrTokenActor && !metadataOperation && actorFqName != null) {
            log.info("Operation failed, operation is not exposed to the given actor");
            throw new AccessDeniedException(ValidationResult.builder()
                    .code("ACCESS_DENIED")
                    .level(ValidationResult.Level.ERROR)
                    .build());
        } else if (!exposedForPublicOrTokenActor && !metadataOperation) {
            log.info("Operation failed, authentication token is required to call a non-public operation");
            throw new AuthenticationRequiredException(ValidationResult.builder()
                    .code("AUTHENTICATION_REQUIRED")
                    .level(ValidationResult.Level.ERROR)
                    .build());
        }

        if (signedIdentifier != null && signedIdentifier.getProducedBy() != null && AsmUtils.getExtensionAnnotationListByName(signedIdentifier.getProducedBy(), "exposedBy").stream()
                .noneMatch(a -> publicActors.contains(a.getDetails().get("value")) || Objects.equals(actorFqName, a.getDetails().get("value")))) {
            log.info("Operation failed, principal has no permission to access instance of bound operation");
            throw new AccessDeniedException(ValidationResult.builder()
                    .code("ACCESS_DENIED_FOR_INSTANCE_OF_BOUND_OPERATION")
                    .level(ValidationResult.Level.ERROR)
                    .build());
        }

        authorizers.stream()
                .filter(authorizer -> authorizer.isSuitableForOperation(operation))
                .forEach(authorizer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Authorizing {} on {}", actorFqName, AsmUtils.getOperationFQName(operation));
                    }
                    authorizer.authorize(actorFqName, publicActors, signedIdentifier, operation);
                });
    }
}
