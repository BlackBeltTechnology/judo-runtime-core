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
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dispatcher.CallInterceptorUtil;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultDispatcher;
import hu.blackbelt.judo.runtime.core.dispatcher.Export;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.eclipse.emf.ecore.*;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class ExportCall<ID> extends AlwaysRollbackTransactionalBehaviourCall<ID> {

    final DAO<ID> dao;
    final AsmUtils asmUtils;
    final IdentifierProvider<ID> identifierProvider;
    final Coercer coercer;
    final ActorResolver actorResolver;
    final Export exporter;

    private final QueryCustomizerParameterProcessor<ID> queryCustomizerParameterProcessor;

    public ExportCall(Context context, DAO<ID> dao, IdentifierProvider<ID> identifierProvider, AsmModel asmModel,
                      final PlatformTransactionManager transactionManager, final OperationCallInterceptorProvider interceptorProvider,
                      final Coercer coercer, final ActorResolver actorResolver, boolean caseInsensitiveLike, final Export exporter) {
        super(context, transactionManager, interceptorProvider, asmModel);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());
        this.coercer = coercer;
        this.actorResolver = actorResolver;
        this.exporter = exporter;
        queryCustomizerParameterProcessor = new QueryCustomizerParameterProcessor<>(asmUtils, caseInsensitiveLike, identifierProvider, coercer);
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.EXPORT).isPresent();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object callInRollbackTransaction(final Map<String, Object> exchange, final EOperation operation) {

        CallInterceptorUtil<ExportCallPayload<ID>, Object> callInterceptorUtil = new CallInterceptorUtil<>(
                ExportCallPayload.class, Object.class, asmModel, operation, interceptorProvider
        );

        final boolean bound = AsmUtils.isBound(operation);

        final EReference owner = (EReference) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final Optional<Map<String, Object>> queryCustomizerParameter = operation.getEParameters().stream().map(ENamedElement::getName)
                .findFirst()
                .map(inputParameter -> (Map<String, Object>) exchange.get(inputParameter));

        final DAO.QueryCustomizer<ID> queryCustomizer = queryCustomizerParameterProcessor.build(queryCustomizerParameter.orElse(null), owner.getEReferenceType());

        ExportCallPayload<ID> inputParameter = callInterceptorUtil.preCallInterceptors(ExportCallPayload.<ID>builder()
                        .instance(Payload.asPayload(exchange))
                        .owner(owner)
                        .queryCustomizer(queryCustomizer)
                        .build());

        Object result = null;
        List<Payload> resultPayload = null;

        if (callInterceptorUtil.shouldCallOriginal()) {
            if (AsmUtils.annotatedAsTrue(inputParameter.getOwner(), "access")
                    && inputParameter.getOwner().isDerived()
                    && asmUtils.isMappedTransferObjectType(inputParameter.getOwner().getEContainingClass())) {
                checkArgument(!bound, "Operation must be unbound");

                final Map<String, Object> actor;
                if (exchange.containsKey(Dispatcher.ACTOR_KEY)) {
                    actor = (Map<String, Object>) exchange.get(Dispatcher.ACTOR_KEY);
                } else if (exchange.get(Dispatcher.PRINCIPAL_KEY) instanceof JudoPrincipal) {
                    actor = actorResolver.authenticateByPrincipal((JudoPrincipal) exchange.get(Dispatcher.PRINCIPAL_KEY))
                            .orElseThrow(() -> new IllegalArgumentException("Unknown actor"));
                } else {
                    throw new IllegalStateException("Unknown or unsupported actor");
                }

                final ID id = (ID) actor.get(identifierProvider.getName());

                resultPayload = dao.searchNavigationResultAt(id, owner, queryCustomizer);

            } else if (AsmUtils.annotatedAsTrue(owner, "access") || !asmUtils.isMappedTransferObjectType(owner.getEContainingClass())) {
                checkArgument(!bound, "Operation must be unbound");
                final List<Payload> resultList;
                if (AsmUtils.annotatedAsTrue(owner, "access") && !owner.isDerived()) {
                    resultPayload = dao.search(owner.getEReferenceType(), queryCustomizer);
                } else {
                    resultPayload = dao.searchReferencedInstancesOf(owner, owner.getEReferenceType(), queryCustomizer);
                }
            } else {
                checkArgument(bound, "Operation must be bound");

                final Optional<Optional<Object>> resultInThis = Optional.ofNullable(exchange.get(Dispatcher.INSTANCE_KEY_OF_BOUND_OPERATION))
                        .filter(_this -> _this instanceof Payload && ((Payload) _this).containsKey(owner.getName()))
                        .map(_this -> Optional.ofNullable(((Payload) _this).get(owner.getName())));

                if (resultInThis.isPresent()) {
                    result = resultInThis.get().orElse(null);

                    if(result != null && result instanceof Payload) {
                        resultPayload = List.of((Payload) result);
                    }
                } else {
                    resultPayload = dao.searchNavigationResultAt((ID) exchange.get(identifierProvider.getName()), owner, queryCustomizer);
                }
            }

            if (resultPayload != null) {
                List<String> mask = queryCustomizer.getMask().keySet().stream().toList();
                String fqName = operation.getEGenericType().getEClassifier().getName();
                try {
                    result = exporter.exportToInputStream(null,
                            resultPayload,
                            mask,
                            null,
                            asmModel,
                            fqName);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (inputParameter.getRecordCount() > -1) {
            exchange.put(DefaultDispatcher.RECORD_COUNT_KEY, inputParameter.getRecordCount());
        }
        return callInterceptorUtil.postCallInterceptors(inputParameter, result);
    }

    @Builder
    @Getter
    public static class ExportCallPayload<ID> {
        @NonNull
        EReference owner;

        @NonNull
        Payload instance;

        DAO.QueryCustomizer<ID> queryCustomizer;

        @Setter
        @Builder.Default
        long recordCount = -1L;

    }

}
