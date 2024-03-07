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
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;

import org.springframework.transaction.PlatformTransactionManager;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class UnsetReferenceCall<ID> extends TransactionalBehaviourCall<ID> {

    final ServiceContext serviceContext;

    public UnsetReferenceCall(Context context, ServiceContext<ID> serviceContext) {
        super(context, serviceContext.getTransactionManager(), serviceContext.getInterceptorProvider(), serviceContext.getAsmModel());
        this.serviceContext = serviceContext;
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.UNSET_REFERENCE).isPresent();
    }

    @Override
    public Object callInTransaction(Map<String, Object> exchange, EOperation operation) {
        CallInterceptorUtil<UnsetReferenceCallPayload, Void> callInterceptorUtil = new CallInterceptorUtil<>(
                UnsetReferenceCallPayload.class, Void.class, asmModel, operation, interceptorProvider);

        final EReference owner = (EReference) serviceContext.getAsmUtils().getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final boolean bound = AsmUtils.isBound(operation);
        checkArgument(bound, "Operation must be bound");

        UnsetReferenceCallPayload inputParameter = callInterceptorUtil.preCallInterceptors(
                UnsetReferenceCallPayload.builder()
                        .owner(owner)
                        .instance(Payload.asPayload(exchange))
                        .build());

        if (callInterceptorUtil.shouldCallOriginal()) {
            @SuppressWarnings("unchecked")
            final ID instanceId = (ID) inputParameter.getInstance().get(serviceContext.getIdentifierProvider().getName());
            serviceContext.getDao().unsetReference(owner, instanceId);
        }

        return callInterceptorUtil.postCallInterceptors(inputParameter, null);
    }

    @Builder
    @Getter
    public static class UnsetReferenceCallPayload {
        @NonNull
        EReference owner;

        @NonNull
        Payload instance;
    }

}
