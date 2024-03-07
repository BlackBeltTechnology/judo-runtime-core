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
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.expression.support.ExpressionModelResourceSupport;
import hu.blackbelt.judo.runtime.core.dispatcher.CallInterceptorUtil;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultDispatcher;
import lombok.*;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

public class GetInputRangeCall<ID> extends AlwaysRollbackTransactionalBehaviourCall<ID> {

    final ServiceContext serviceContext;

    private final QueryCustomizerParameterProcessor<ID> queryCustomizerParameterProcessor;

    final ExpressionModelResourceSupport expressionModelResourceSupport;
    private final MarkedIdRemover<ID> markedIdRemover;
    private final CollectedIdRemover<ID> collectedIdRemover;

    private static final String OWNER_KEY = "owner";
    private static final String QUERY_CUSTOMIZER_KEY = "queryCustomizer";

    @SneakyThrows
    public GetInputRangeCall(Context context, ServiceContext serviceContext,
                             ExpressionModel expressionModel) {
        super(context, serviceContext.getTransactionManager(), serviceContext.getInterceptorProvider(), serviceContext.getAsmModel());
        this.serviceContext = serviceContext;
        queryCustomizerParameterProcessor = new QueryCustomizerParameterProcessor<>(
                serviceContext.getAsmUtils(),
                serviceContext.isCaseInsensitiveLike(),
                serviceContext.getIdentifierProvider(),
                serviceContext.getCoercer());

        this.markedIdRemover = new MarkedIdRemover<>(serviceContext.getIdentifierProvider().getName());
        this.collectedIdRemover = new CollectedIdRemover<>(serviceContext.getIdentifierProvider().getName());

        this.expressionModelResourceSupport = ExpressionModelResourceSupport.expressionModelResourceSupportBuilder()
                .resourceSet(expressionModel.getResourceSet())
                .uri(expressionModel.getUri()).build();
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.GET_INPUT_RANGE).isPresent();
    }

    @Override
    public Object callInRollbackTransaction(Map<String, Object> exchange, EOperation operation) {
        CallInterceptorUtil<GetInputRangeCallPayload<ID>, Collection<Payload>> callInterceptorUtil = new CallInterceptorUtil<>(
                GetInputRangeCallPayload.class, Collection.class, asmModel, operation, interceptorProvider);

        final EOperation owner = (EOperation) serviceContext.getAsmUtils().getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));
        String inputRangeReferenceFQName = AsmUtils.getExtensionAnnotationValue(owner, "inputRange", false)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));
        final EReference inputRangeReference = serviceContext.getAsmUtils().resolveReference(inputRangeReferenceFQName)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final Optional<String> inputParameterName = operation.getEParameters().stream().map(ENamedElement::getName).findFirst();
        @SuppressWarnings("unchecked")

        final Map<String, Object> inputData = (Map<String, Object>) exchange.get(inputParameterName.orElse(null));
        @SuppressWarnings({"unchecked"})
        Map<String, Object> queryCustomizerData = inputData != null ? (Map<String, Object>) inputData.get(QUERY_CUSTOMIZER_KEY) : null;

        @SuppressWarnings({"unchecked"})
        Payload ownerPayload = inputParameterName
                .filter(parameterName -> exchange.get(parameterName) != null)
                .map(parameterName -> Payload.asPayload((Map<String, Object>) exchange.get(parameterName)).getAsPayload(OWNER_KEY))
                .orElse(null);


        final DAO.QueryCustomizer<ID> queryCustomizer = queryCustomizerParameterProcessor.build(
                queryCustomizerData, inputRangeReference.getEReferenceType(), exchange);

        GetInputRangeCallPayload<ID> inputParameter = callInterceptorUtil.preCallInterceptors(
                        GetInputRangeCallPayload.<ID>builder()
                                .reference(inputRangeReference)
                                .ownerPayload(ownerPayload)
                                .queryCustomizer(queryCustomizer)
                                .build());

        Collection<Payload> result = new ArrayList<>();

        if (callInterceptorUtil.shouldCallOriginal()) {
            final boolean bound = AsmUtils.isBound(operation);
            checkArgument(!bound, "Operation must be unbound");

            final Collection<ID> idsToRemove = new HashSet<>();
            result = serviceContext.getDao().getRangeOf(
                    inputParameter.getReference(),
                    inputParameter.getOwnerPayload(),
                    inputParameter.getQueryCustomizer(),
                    true);

            if (Boolean.TRUE.equals(exchange.get(DefaultDispatcher.COUNT_QUERY_RECORD_KEY))) {
                inputParameter.setRecordCount(serviceContext.getDao().countRangeOf(
                        inputParameter.getReference(),
                        inputParameter.getOwnerPayload(),
                        inputParameter.getQueryCustomizer(),
                        true
                        ));
            }

            // collect IDs that are created (temporary)
            result.forEach(p -> markedIdRemover.processAndCollect(p, idsToRemove));
            // remove identifiers of temporary instances (keep identifiers of instances existing before operation call)
            result.forEach(payload -> collectedIdRemover.removeIdentifiers(payload, idsToRemove));
        }


        if (inputParameter.getRecordCount() > -1) {
            exchange.put(DefaultDispatcher.RECORD_COUNT_KEY, inputParameter.getRecordCount());
        }
        return callInterceptorUtil.postCallInterceptors(inputParameter, result);
    }

    @Builder
    @Getter
    public static class GetInputRangeCallPayload<ID> {
        @NonNull
        EReference reference;

        Payload ownerPayload;

        DAO.QueryCustomizer<ID> queryCustomizer;

        @Setter
        @Builder.Default
        long recordCount = -1L;

    }

}
