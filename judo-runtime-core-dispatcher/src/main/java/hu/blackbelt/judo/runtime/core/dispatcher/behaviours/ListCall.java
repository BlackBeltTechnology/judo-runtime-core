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
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.mapper.api.Coercer;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;
import org.springframework.transaction.PlatformTransactionManager;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class ListCall<ID> extends AlwaysRollbackTransactionalBehaviourCall<ID> {

    final DAO<ID> dao;
    final AsmUtils asmUtils;
    final IdentifierProvider<ID> identifierProvider;
    final Coercer coercer;
    final ActorResolver actorResolver;

    private final QueryCustomizerParameterProcessor<ID> queryCustomizerParameterProcessor;

    public ListCall(Context context, DAO<ID> dao, IdentifierProvider<ID> identifierProvider, AsmModel asmModel,
                    final PlatformTransactionManager transactionManager, final OperationCallInterceptorProvider interceptorProvider,
                    final Coercer coercer, final ActorResolver actorResolver, boolean caseInsensitiveLike) {
        super(context, transactionManager, interceptorProvider, asmModel);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());
        this.coercer = coercer;
        this.actorResolver = actorResolver;
        queryCustomizerParameterProcessor = new QueryCustomizerParameterProcessor<ID>(asmUtils, caseInsensitiveLike, identifierProvider, coercer);
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.LIST).isPresent();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object callInRollbackTransaction(final Map<String, Object> exchange, final EOperation operation) {
        final boolean bound = AsmUtils.isBound(operation);
        final EReference owner = (EReference) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        Object result = null;
        Long count = null;

        final Optional<Map<String, Object>> queryCustomizerParameter = (Optional<Map<String, Object>>) CallInterceptorUtil.preCallInterceptors(asmModel, operation, interceptorProvider,
                operation.getEParameters().stream().map(p -> p.getName())
                        .findFirst()
                        .map(inputParameter -> (Map<String, Object>) exchange.get(inputParameter)));

        if (CallInterceptorUtil.isOriginalCalled(asmModel, operation, interceptorProvider)) {
            @SuppressWarnings("rawtypes")
            final DAO.QueryCustomizer queryCustomizer = queryCustomizerParameterProcessor.build(queryCustomizerParameter.orElse(null), owner.getEReferenceType());

            final boolean countRecords = Boolean.TRUE.equals(exchange.get(DefaultDispatcher.COUNT_QUERY_RECORD_KEY));

            if (AsmUtils.annotatedAsTrue(owner, "access") && owner.isDerived() && asmUtils.isMappedTransferObjectType(owner.getEContainingClass())) {
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
                result = extractResult(operation, dao.searchNavigationResultAt(id, owner, queryCustomizer));
                if (countRecords) {
                    count = dao.countNavigationResultAt(id, owner, queryCustomizer);
                }

            } else if (AsmUtils.annotatedAsTrue(owner, "access") || !asmUtils.isMappedTransferObjectType(owner.getEContainingClass())) {
                checkArgument(!bound, "Operation must be unbound");
                final List<Payload> resultList;
                if (AsmUtils.annotatedAsTrue(owner, "access") && !owner.isDerived()) {
                    resultList = dao.search(owner.getEReferenceType(), queryCustomizer);
                    if (countRecords) {
                        count = dao.count(owner.getEReferenceType(), queryCustomizer);
                    }
                } else {
                    resultList = dao.searchReferencedInstancesOf(owner, owner.getEReferenceType(), queryCustomizer);
                    if (countRecords) {
                        count = dao.countReferencedInstancesOf(owner, owner.getEReferenceType(), queryCustomizer);
                    }

                }
                result = extractResult(operation, resultList);
            } else {
                checkArgument(bound, "Operation must be bound");

                final Optional<Optional<Object>> resultInThis = Optional.ofNullable(exchange.get(Dispatcher.INSTANCE_KEY_OF_BOUND_OPERATION))
                        .filter(_this -> _this instanceof Payload && ((Payload) _this).containsKey(owner.getName()))
                        .map(_this -> Optional.ofNullable(((Payload) _this).get(owner.getName())));

                if (resultInThis.isPresent()) {
                    result = resultInThis.get().orElse(null);
                } else {
                    result = extractResult(operation, dao.searchNavigationResultAt((ID) exchange.get(identifierProvider.getName()), owner, queryCustomizer));
                    if (countRecords) {
                        count = dao.countNavigationResultAt((ID) exchange.get(identifierProvider.getName()), owner, queryCustomizer);
                    }
                }
            }
            if (count != null) {
                exchange.put(DefaultDispatcher.RECORD_COUNT_KEY, count);
            }
        }

        return CallInterceptorUtil.postCallInterceptors(asmModel, operation, interceptorProvider, queryCustomizerParameter, result);
    }

    private Object extractResult(final EOperation operation, final List<Payload> resultList) {
        if (operation.isMany()) {
            return resultList;
        } else {
            if (operation.isRequired()) {
                checkArgument(!resultList.isEmpty(), "No record found");
                return resultList.get(0);
            } else {
                return resultList.isEmpty() ? null : resultList.get(0);
            }
        }
    }
}
