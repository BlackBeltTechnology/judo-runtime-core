package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

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

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dispatcher.CallInterceptorUtil;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;
import org.springframework.transaction.PlatformTransactionManager;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class RemoveReferenceCall<ID> extends TransactionalBehaviourCall<ID> {

    final ServiceContext serviceContext;

    public RemoveReferenceCall(Context context, ServiceContext<ID> serviceContext) {
        super(context, serviceContext.getTransactionManager(), serviceContext.getInterceptorProvider(), serviceContext.getAsmModel());
        this.serviceContext = serviceContext;
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.REMOVE_REFERENCE).isPresent();
    }

    @Override
    public Object callInTransaction(Map<String, Object> exchange, EOperation operation) {
        CallInterceptorUtil<RemoveReferenceCallPayload, Void> callInterceptorUtil = new CallInterceptorUtil<>(
                RemoveReferenceCallPayload.class, Void.class, asmModel, operation, interceptorProvider);

        final EReference owner = (EReference) serviceContext.getAsmUtils().getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final String inputParameterName = operation.getEParameters().stream().map(ENamedElement::getName).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Input parameter name must be defined"));

        final boolean bound = AsmUtils.isBound(operation);
        checkArgument(bound, "Operation must be bound");

        @SuppressWarnings({"unchecked"})
        RemoveReferenceCallPayload inputParameter = callInterceptorUtil.preCallInterceptors(
                RemoveReferenceCallPayload.builder()
                        .instance(Payload.asPayload(exchange))
                        .owner(owner)
                        .references(((Collection<Map<String, Object>>) exchange.get(inputParameterName)).stream()
                                .map(Payload::asPayload).collect(Collectors.toList()))
                        .build());


        if (callInterceptorUtil.shouldCallOriginal()) {
            @SuppressWarnings({"unchecked"})
            final Collection<ID> referencedIds = inputParameter.getReferences().stream()
                    .map(p -> (ID) p.get(serviceContext.getIdentifierProvider().getName()))
                    .collect(Collectors.toList());

            @SuppressWarnings({"unchecked"})
            ID instanceId = (ID) inputParameter.getInstance().get(serviceContext.getIdentifierProvider().getName());
            serviceContext.getDao().removeReferences(inputParameter.getOwner(),
                    instanceId,
                    referencedIds);
        }

        return callInterceptorUtil.postCallInterceptors(inputParameter, null);
    }

    @Builder
    @Getter
    public static class RemoveReferenceCallPayload {
        @NonNull
        EReference owner;

        @NonNull
        Payload instance;

        @NonNull
        Collection<Payload> references;
    }

}
