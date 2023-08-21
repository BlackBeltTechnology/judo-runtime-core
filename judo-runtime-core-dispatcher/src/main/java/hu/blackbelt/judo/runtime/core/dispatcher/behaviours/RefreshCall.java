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
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dispatcher.CallInterceptorUtil;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import hu.blackbelt.mapper.api.Coercer;
import org.eclipse.emf.ecore.EClass;
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
        queryCustomizerParameterProcessor = new QueryCustomizerParameterProcessor<ID>(asmUtils, caseInsensitiveLike, identifierProvider, coercer);
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.REFRESH).isPresent();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object callInRollbackTransaction(Map<String, Object> exchange, EOperation operation) {
        final EClass owner = (EClass) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final boolean bound = AsmUtils.isBound(operation);
        checkArgument(bound, "Operation must be bound");

        final Optional<Map<String, Object>> queryCustomizerParameter = (Optional<Map<String, Object>>) CallInterceptorUtil.preCallInterceptors(asmModel, operation, interceptorProvider,
                operation.getEParameters().stream().map(p -> p.getName())
                        .findFirst()
                        .map(inputParameter -> (Map<String, Object>) exchange.get(inputParameter)));
        Object result = null;
        if (CallInterceptorUtil.isOriginalCalled(asmModel, operation, interceptorProvider)) {
            final DAO.QueryCustomizer queryCustomizer = queryCustomizerParameterProcessor.build(queryCustomizerParameter.orElse(null), owner);
            result = dao.searchByIdentifier(owner, (ID) exchange.get(identifierProvider.getName()), queryCustomizer).get();

        }

        return CallInterceptorUtil.postCallInterceptors(asmModel, operation, interceptorProvider, queryCustomizerParameter, result);
    }
}
