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
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EOperation;

import org.springframework.transaction.PlatformTransactionManager;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.dao.api.Payload.asPayload;

public class UpdateInstanceCall<ID> extends TransactionalBehaviourCall<ID> {

    final ServiceContext<ID> serviceContext;
    private final QueryCustomizerParameterProcessor<ID> queryCustomizerParameterProcessor;

    public UpdateInstanceCall(Context context, ServiceContext<ID> serviceContext) {
        super(context, serviceContext.getTransactionManager(), serviceContext.getInterceptorProvider(), serviceContext.getAsmModel());
        this.serviceContext = serviceContext;
        queryCustomizerParameterProcessor = new QueryCustomizerParameterProcessor<>(
                serviceContext.getAsmUtils(),
                serviceContext.isCaseInsensitiveLike(),
                serviceContext.getIdentifierProvider(),
                serviceContext.getCoercer());
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.UPDATE_INSTANCE).isPresent();
    }

    @Override
    public Object callInTransaction(Map<String, Object> exchange, EOperation operation) {
        CallInterceptorUtil<UpdateInstanceCallPayload, Payload> callInterceptorUtil = new CallInterceptorUtil<>(
                UpdateInstanceCallPayload.class, Payload.class, asmModel, operation, interceptorProvider);

        final EClass owner = (EClass) serviceContext.getAsmUtils().getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final String inputParameterName = operation.getEParameters().stream().map(ENamedElement::getName).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Input parameter name must be defined"));

        final DAO.QueryCustomizer<ID> queryCustomizer = queryCustomizerParameterProcessor
                .build(null, owner, exchange);

        final boolean bound = AsmUtils.isBound(operation);
        checkArgument(bound, "Operation must be bound");

        @SuppressWarnings("unchecked")
        final Payload payload = asPayload((Map<String, Object>) exchange.get(inputParameterName));
        if (payload.get(serviceContext.getIdentifierProvider().getName()) == null) {
            payload.put(serviceContext.getIdentifierProvider().getName(), exchange.get(serviceContext.getIdentifierProvider().getName()));
        } else {
            payload.put(serviceContext.getIdentifierProvider().getName(),
                    serviceContext.getCoercer().coerce(exchange.get(serviceContext.getIdentifierProvider().getName()),
                            serviceContext.getIdentifierProvider().getType()));

            @SuppressWarnings("unchecked")
            final ID idInPayload = (ID) payload.get(serviceContext.getIdentifierProvider().getName());
            @SuppressWarnings("unchecked")
            final ID idOfSubject = (ID) exchange.get(serviceContext.getIdentifierProvider().getName());

            if (!Objects.equals(idInPayload, idOfSubject)) {
                throw new IllegalArgumentException("Identifier in payload must match operation subject");
            }
        }

        UpdateInstanceCallPayload inputParameter =
                callInterceptorUtil.preCallInterceptors(
                        UpdateInstanceCallPayload.builder()
                                .owner(owner)
                                .input(payload)
                                .instance(Payload.asPayload(exchange))
                                .build());

        Payload result = null;
        if (callInterceptorUtil.shouldCallOriginal()) {
            result =  serviceContext.getDao().update(
                    inputParameter.getOwner(),
                    inputParameter.getInput(), queryCustomizer);
        }
        return callInterceptorUtil.postCallInterceptors(inputParameter, result);
    }

    @Builder
    @Getter
    public static class UpdateInstanceCallPayload {
        @NonNull
        EClass owner;

        @NonNull
        Payload input;

        @NonNull
        Payload instance;

    }

}
