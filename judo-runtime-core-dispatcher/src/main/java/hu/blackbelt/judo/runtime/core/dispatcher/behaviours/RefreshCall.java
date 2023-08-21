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
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class RefreshCall<ID> extends AlwaysRollbackTransactionalBehaviourCall<ID> {

    final DAO<ID> dao;
    final IdentifierProvider<ID> identifierProvider;
    final AsmUtils asmUtils;
    private final QueryCustomizerParameterProcessor<ID> queryCustomizerParameterProcessor;

    public RefreshCall(Context context, DAO<ID> dao, IdentifierProvider<ID> identifierProvider, AsmModel asmModel,
                       PlatformTransactionManager transactionManager, OperationCallInterceptorProvider interceptorProvider,
                       Coercer coercer, boolean caseInsensitiveLike) {
        super(context, transactionManager, interceptorProvider, asmModel);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());
        queryCustomizerParameterProcessor = new QueryCustomizerParameterProcessor<>(asmUtils, caseInsensitiveLike, identifierProvider, coercer);
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.REFRESH).isPresent();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object callInRollbackTransaction(Map<String, Object> exchange, EOperation operation) {
        CallInterceptorUtil<RefreshCallPayload<ID>, Payload> callInterceptorUtil = new CallInterceptorUtil<>(
                RefreshCallPayload.class, Payload.class, asmModel, operation, interceptorProvider
        );

        final EClass owner = (EClass) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final boolean bound = AsmUtils.isBound(operation);
        checkArgument(bound, "Operation must be bound");

        Optional<Map<String, Object>> queryCustomizerParameter = operation.getEParameters().stream()
                .map(ENamedElement::getName)
                .findFirst()
                .map(inputParameter -> (Map<String, Object>) exchange.get(inputParameter));

        final DAO.QueryCustomizer<ID> queryCustomizer = queryCustomizerParameterProcessor
                .build(queryCustomizerParameter.orElse(null), owner);


        RefreshCallPayload<ID> inputParameter = callInterceptorUtil.preCallInterceptors(RefreshCallPayload.<ID>builder()
                .owner(owner)
                .instance(Payload.asPayload(exchange))
                .queryCustomizer(queryCustomizer)
                .build());

        Optional<Payload> result = Optional.empty();

        if (callInterceptorUtil.isOriginalCalled()) {
            result = dao.searchByIdentifier(inputParameter.getOwner(),
                    (ID) inputParameter.getInstance().get(identifierProvider.getName()),
                    inputParameter.getQueryCustomizer());
        }
        return callInterceptorUtil.postCallInterceptors(inputParameter, result.orElse(null));
    }

    @Builder
    @Getter
    public static class RefreshCallPayload<ID> {
        @NonNull
        EClass owner;

        @NonNull
        Payload instance;

        DAO.QueryCustomizer<ID> queryCustomizer;
    }

}
