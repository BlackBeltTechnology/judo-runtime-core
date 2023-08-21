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
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;
import org.springframework.transaction.PlatformTransactionManager;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.dao.api.Payload.asPayload;

public class ValidateCreateCall<ID> extends AlwaysRollbackTransactionalBehaviourCall<ID> {

    final DAO<ID> dao;
    final AsmUtils asmUtils;
    final IdentifierProvider<ID> identifierProvider;

    private final MarkedIdRemover<ID> markedIdRemover;

    public ValidateCreateCall(Context context, DAO<ID> dao, IdentifierProvider<ID> identifierProvider, AsmModel asmModel,
                              PlatformTransactionManager transactionManager, OperationCallInterceptorProvider interceptorProvider) {
        super(context, transactionManager, interceptorProvider, asmModel);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());
        markedIdRemover = new MarkedIdRemover<>(identifierProvider.getName());
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.VALIDATE_CREATE).isPresent();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object callInRollbackTransaction(Map<String, Object> exchange, EOperation operation) {
        final EReference owner = (EReference) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final String inputParameterName = operation.getEParameters().stream().map(p -> p.getName()).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Input parameter name must be defined"));

        final boolean bound = AsmUtils.isBound(operation);

        final Payload payload = asPayload((Map<String, Object>) exchange.get(inputParameterName));

        final Payload result;
        if (AsmUtils.annotatedAsTrue(owner, "access") || !asmUtils.isMappedTransferObjectType(owner.getEContainingClass())) {
            checkArgument(!bound, "Operation must be unbound");
            result = dao.create(owner.getEReferenceType(), payload, null);
        } else {
            checkArgument(bound, "Operation must be bound");
            result = dao.createNavigationInstanceAt((ID) exchange.get(identifierProvider.getName()), owner, payload, null);
        }

        markedIdRemover.process(result);
        return result;
    }
}
