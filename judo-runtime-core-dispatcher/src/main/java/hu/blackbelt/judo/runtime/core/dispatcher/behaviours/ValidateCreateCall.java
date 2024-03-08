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
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dispatcher.CallInterceptorUtil;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.dao.api.Payload.asPayload;

public class ValidateCreateCall<ID> extends AlwaysRollbackTransactionalBehaviourCall<ID> {

    final ServiceContext<ID> serviceContext;
    private final QueryCustomizerParameterProcessor<ID> queryCustomizerParameterProcessor;

    private final MarkedIdRemover<ID> markedIdRemover;

    public ValidateCreateCall(Context context, ServiceContext<ID> serviceContext) {
        super(context, serviceContext.getTransactionManager(), serviceContext.getInterceptorProvider(), serviceContext.getAsmModel());
        this.serviceContext = serviceContext;
        queryCustomizerParameterProcessor = new QueryCustomizerParameterProcessor<>(
                serviceContext.getAsmUtils(),
                serviceContext.isCaseInsensitiveLike(),
                serviceContext.getIdentifierProvider(),
                serviceContext.getCoercer());
        markedIdRemover = new MarkedIdRemover<>(serviceContext.getIdentifierProvider().getName());
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.VALIDATE_CREATE).isPresent();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object callInRollbackTransaction(Map<String, Object> exchange, EOperation operation) {
        CallInterceptorUtil<ValidateCreateCallPayload, Payload> callInterceptorUtil = new CallInterceptorUtil<>(
                ValidateCreateCallPayload.class, Payload.class, asmModel, operation, interceptorProvider);

        final EReference owner = (EReference) serviceContext.getAsmUtils().getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final String inputParameterName = operation.getEParameters().stream().map(ENamedElement::getName).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Input parameter name must be defined"));

        final DAO.QueryCustomizer<ID> queryCustomizer = queryCustomizerParameterProcessor
                .build(null, owner.getEReferenceType(), exchange);

        ValidateCreateCallPayload inputParameter =
                callInterceptorUtil.preCallInterceptors(
                        ValidateCreateCallPayload.builder()
                                .owner(owner)
                                .instance(Payload.asPayload(exchange))
                                .input(asPayload((Map<String, Object>) exchange.get(inputParameterName)))
                                .build());

        Payload result = null;
        if (callInterceptorUtil.shouldCallOriginal()) {
            final boolean bound = AsmUtils.isBound(operation);

            if (AsmUtils.annotatedAsTrue(inputParameter.getOwner(), "access") ||
                    !serviceContext.getAsmUtils().isMappedTransferObjectType(owner.getEContainingClass())) {
                checkArgument(!bound, "Operation must be unbound");
                result = serviceContext.getDao().create(
                        inputParameter.getOwner().getEReferenceType(),
                        inputParameter.getInput(), queryCustomizer);
            } else {
                checkArgument(bound, "Operation must be bound");
                result = serviceContext.getDao().createNavigationInstanceAt(
                        (ID) inputParameter.getInstance().get(serviceContext.getIdentifierProvider().getName()),
                        inputParameter.getOwner(),
                        inputParameter.getInput(), queryCustomizer);
            }
        }

        markedIdRemover.process(result);
        return callInterceptorUtil.postCallInterceptors(inputParameter, result);
    }

    @Builder
    @Getter
    public static class ValidateCreateCallPayload {
        @NonNull
        EReference owner;

        @NonNull
        Payload input;

        Payload instance;
    }

}
