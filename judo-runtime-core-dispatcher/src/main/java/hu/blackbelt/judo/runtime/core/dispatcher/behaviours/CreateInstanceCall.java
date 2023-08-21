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

import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.dao.api.Payload.asPayload;

public class CreateInstanceCall<ID> extends TransactionalBehaviourCall<ID> {

    final DAO<ID> dao;
    final AsmUtils asmUtils;
    final IdentifierProvider<ID> identifierProvider;

    public CreateInstanceCall(Context context, DAO<ID> dao, IdentifierProvider<ID> identifierProvider,
                              AsmModel asmModel, PlatformTransactionManager transactionManager,
                              OperationCallInterceptorProvider interceptorProvider) {
        super(context, transactionManager, interceptorProvider, asmModel);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.CREATE_INSTANCE).isPresent();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object callInTransaction(Map<String, Object> exchange, EOperation operation) {
        final EReference owner = (EReference) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final String inputParameterName = operation.getEParameters().stream().map(p -> p.getName()).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Input parameter name must be defined"));

        CreateInstanceCallPayload<ID> createInstanceCallPayload =
                CallInterceptorUtil.preCallInterceptors(CreateInstanceCallPayload.class, asmModel, operation, interceptorProvider,
                        CreateInstanceCallPayload.builder()
                                .owner(owner)
                                .ownerId(exchange.get(identifierProvider.getName()))
                                .input(asPayload((Map<String, Object>) exchange.get(inputParameterName)))
                                .build());

        Payload ret = null;
        if (CallInterceptorUtil.isOriginalCalled(asmModel, operation, interceptorProvider)) {
            final boolean bound = AsmUtils.isBound(operation);

            if (AsmUtils.annotatedAsTrue(owner, "access") || !asmUtils.isMappedTransferObjectType(owner.getEContainingClass())) {
                checkArgument(!bound, "Operation must be unbound");
                ret = dao.create(owner.getEReferenceType(),
                        createInstanceCallPayload.getInput(), null);
            } else {
                checkArgument(bound, "Operation must be bound");
                ret = dao.createNavigationInstanceAt(createInstanceCallPayload.getOwnerId(),
                        createInstanceCallPayload.getOwner(),
                        createInstanceCallPayload.getInput(), null);
            }
        }
        return CallInterceptorUtil.postCallInterceptors(CreateInstanceCallPayload.class, Payload.class, asmModel, operation,
                interceptorProvider, createInstanceCallPayload, ret);
    }

    @Builder
    @Getter
    public static class CreateInstanceCallPayload<ID> {
        @NonNull
        EReference owner;

        @NonNull
        Payload input;

        ID ownerId;
    }

}
