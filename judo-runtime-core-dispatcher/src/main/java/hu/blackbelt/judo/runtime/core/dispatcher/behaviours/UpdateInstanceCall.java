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
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import hu.blackbelt.mapper.api.Coercer;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EOperation;

import org.springframework.transaction.PlatformTransactionManager;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.dao.api.Payload.asPayload;

public class UpdateInstanceCall<ID> extends TransactionalBehaviourCall<ID> {

    final DAO<ID> dao;
    final IdentifierProvider<ID> identifierProvider;
    final AsmUtils asmUtils;
    final Coercer coercer;

    public UpdateInstanceCall(Context context, DAO<ID> dao, IdentifierProvider<ID> identifierProvider, AsmUtils asmUtils,
                              PlatformTransactionManager transactionManager, OperationCallInterceptorProvider interceptorProvider,
                              Coercer coercer) {
        super(context, transactionManager, interceptorProvider);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = asmUtils;
        this.coercer = coercer;
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.UPDATE_INSTANCE).isPresent();
    }

    @Override
    public Object callInTransaction(Map<String, Object> exchange, EOperation operation) {
        final EClass owner = (EClass) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final String inputParameterName = operation.getEParameters().stream().map(p -> p.getName()).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Input parameter name must be defined"));

        final boolean bound = AsmUtils.isBound(operation);
        checkArgument(bound, "Operation must be bound");

        @SuppressWarnings("unchecked")
        final Payload payload = asPayload((Map<String, Object>) exchange.get(inputParameterName));
        if (payload.get(identifierProvider.getName()) == null) {
            payload.put(identifierProvider.getName(), exchange.get(identifierProvider.getName()));
        } else {
            payload.put(identifierProvider.getName(),
                    coercer.coerce(exchange.get(identifierProvider.getName()), identifierProvider.getType()));

            @SuppressWarnings("unchecked")
            final ID idInPayload = (ID) payload.get(identifierProvider.getName());
            @SuppressWarnings("unchecked")
            final ID idOfSubject = (ID) exchange.get(identifierProvider.getName());

            if (!Objects.equals(idInPayload, idOfSubject)) {
                throw new IllegalArgumentException("Identifier in payload must match operation subject");
            }
        }

        return dao.update(owner, payload, null);
    }
}
