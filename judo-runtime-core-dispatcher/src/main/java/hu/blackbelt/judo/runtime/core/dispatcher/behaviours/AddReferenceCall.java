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

public class AddReferenceCall<ID> extends TransactionalBehaviourCall<ID> {

    final DAO<ID> dao;
    final AsmUtils asmUtils;
    final IdentifierProvider<ID> identifierProvider;

    public AddReferenceCall(Context context, DAO<ID> dao, IdentifierProvider<ID> identifierProvider, AsmModel asmModel,
                            PlatformTransactionManager transactionManager, OperationCallInterceptorProvider interceptorProvider) {
        super(context, transactionManager, interceptorProvider, asmModel);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.ADD_REFERENCE).isPresent();
    }

    @Override
    public Object callInTransaction(Map<String, Object> exchange, EOperation operation) {
        CallInterceptorUtil<AddReferenceCallPayload, Void> callInterceptorUtil = new CallInterceptorUtil<>(
                AddReferenceCallPayload.class, Void.class, asmModel, operation, interceptorProvider);

        final EReference owner = (EReference) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final String inputParameterName = operation.getEParameters().stream().map(ENamedElement::getName).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Input parameter name must be defined"));

        final boolean bound = AsmUtils.isBound(operation);
        checkArgument(bound, "Operation must be bound");

        @SuppressWarnings({"unchecked"})
        AddReferenceCallPayload inputParameter = callInterceptorUtil.preCallInterceptors(
                AddReferenceCallPayload.builder()
                        .instance(Payload.asPayload(exchange))
                        .owner(owner)
                        .references(((Collection<Map<String, Object>>) exchange.get(inputParameterName)).stream()
                                .map(Payload::asPayload).collect(Collectors.toList()))
                        .build());

        if (callInterceptorUtil.isOriginalCalled()) {
            @SuppressWarnings({"unchecked"})
            final Collection<ID> referencedIds = inputParameter.getReferences().stream()
                    .map(p -> (ID) p.get(identifierProvider.getName()))
                    .collect(Collectors.toList());

            @SuppressWarnings({"unchecked"})
            ID instanceId = (ID) inputParameter.getInstance().get(identifierProvider.getName());

            dao.addReferences(inputParameter.getOwner(), instanceId, referencedIds);
        }

        return callInterceptorUtil.postCallInterceptors(inputParameter, null);
    }

    @Builder
    @Getter
    public static class AddReferenceCallPayload {
        @NonNull
        EReference owner;

        @NonNull
        Payload instance;

        @NonNull
        Collection<Payload> references;
    }
}
