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
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EOperation;

import org.springframework.transaction.PlatformTransactionManager;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.isBound;

public class DeleteInstanceCall<ID> extends TransactionalBehaviourCall<ID> {

    final DAO<ID> dao;
    final IdentifierProvider<ID> identifierProvider;
    final AsmUtils asmUtils;

    public DeleteInstanceCall(Context context, DAO<ID> dao, IdentifierProvider<ID> identifierProvider, AsmModel asmModel,
                              PlatformTransactionManager transactionManager, OperationCallInterceptorProvider interceptorProvider) {
        super(context, transactionManager, interceptorProvider, asmModel);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.DELETE_INSTANCE).isPresent();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object callInTransaction(Map<String, Object> exchange, EOperation operation) {
        CallInterceptorUtil<DeleteInstanceCallPayload, Void> callInterceptorUtil = new CallInterceptorUtil<>(
                DeleteInstanceCallPayload.class, Void.class, asmModel, operation, interceptorProvider);

        final EClass owner = (EClass) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final boolean bound = isBound(operation);
        checkArgument(bound, "Operation must be bound");

        DeleteInstanceCallPayload deleteInstanceCallPayload = callInterceptorUtil.preCallInterceptors(
                DeleteInstanceCallPayload.builder()
                        .instance(Payload.asPayload(exchange))
                        .owner(owner)
                        .build());

        if (callInterceptorUtil.shouldCallOriginal()) {
            dao.delete(deleteInstanceCallPayload.getOwner(),
                    (ID) deleteInstanceCallPayload.getInstance().get(identifierProvider.getName()));
        }

        return callInterceptorUtil.postCallInterceptors(deleteInstanceCallPayload, null);
    }

    @Builder
    @Getter
    public static class DeleteInstanceCallPayload {
        @NonNull
        EClass owner;

        @NonNull
        Payload instance;
    }

}
