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

import java.util.Map;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dispatcher.CallInterceptorUtil;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EOperation;


public class ValidateOperationInputCall extends AlwaysRollbackTransactionalBehaviourCall {

    final ServiceContext serviceContext;
    private final MarkedIdRemover markedIdRemover;

    public ValidateOperationInputCall(Context context, ServiceContext serviceContext) {
        super(context, serviceContext.getTransactionManager(), serviceContext.getInterceptorProvider(), serviceContext.getAsmModel());
        this.serviceContext = serviceContext;
        markedIdRemover = new MarkedIdRemover(serviceContext.getIdentifierProvider().getName());
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.VALIDATE_OPERATION_INPUT).isPresent();
    }

    @Override
    public Object callInRollbackTransaction(Map<String, Object> exchange, EOperation operation) {
        final EOperation owner = (EOperation) serviceContext.getAsmUtils().getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        CallInterceptorUtil<ValidateOperationInputCall.ValidateOperationCallPayload, Payload> callInterceptorUtil = new CallInterceptorUtil<>(
                ValidateOperationInputCall.ValidateOperationCallPayload.class, Payload.class, asmModel, operation, interceptorProvider);
        final String inputParameterName = operation.getEParameters().stream().map(ENamedElement::getName).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Input parameter name must be defined"));

        ValidateOperationInputCall.ValidateOperationCallPayload inputParameter =
                callInterceptorUtil.preCallInterceptors(
                        ValidateOperationInputCall.ValidateOperationCallPayload.builder()
                                .owner(owner)
                                .input(Payload.asPayload((Map<String, Object>) exchange.get(inputParameterName)))
                                .build());

        Payload result;
        if (callInterceptorUtil.shouldCallOriginal()) {
            result = Payload.empty();
            serviceContext.getDao().applyDefaultsOf(owner.getEContainingClass(), result);
        } else {
            result = null;
        }

        return callInterceptorUtil.postCallInterceptors(inputParameter, result);
    }


    @Builder
    @Getter
    public static class ValidateOperationCallPayload {
        @NonNull
        EOperation owner;

        @NonNull
        Payload input;

    }

}
