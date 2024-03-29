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

import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import lombok.SneakyThrows;
import org.eclipse.emf.ecore.EOperation;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.util.Map;

public abstract class TransactionalBehaviourCall<ID> implements BehaviourCall<ID> {

    PlatformTransactionManager transactionManager;

    OperationCallInterceptorProvider interceptorProvider;

    AsmModel asmModel;

    public TransactionalBehaviourCall(Context context, PlatformTransactionManager transactionManager,
                                      OperationCallInterceptorProvider interceptorProvider, AsmModel asmModel) {
        this.transactionManager = transactionManager;
        this.interceptorProvider = interceptorProvider;
        this.asmModel = asmModel;
    }

    public abstract Object callInTransaction(Map<String, Object> exchange, EOperation operation);

    @SneakyThrows
    @Override
    public Object call(Map<String, Object> exchange, EOperation operation) {

        TransactionStatus transactionStatus = null;
        if (transactionManager != null) {
            DefaultTransactionDefinition transactionDefinition = new DefaultTransactionDefinition();
            transactionStatus = transactionManager.getTransaction(transactionDefinition);
        }

        try {
            return callInTransaction(exchange, operation);
        } catch (Exception e) {
            if (transactionStatus != null) {
                try {
                    if (transactionStatus.isNewTransaction()) {
                        transactionManager.rollback(transactionStatus);
                    }
                } catch (Exception ex) {
                    throw new IllegalArgumentException("Unable to rollback transaction", ex);
                } finally {
                    transactionStatus = null;
                }
            }
            throw e;
        } finally {
            if (transactionStatus != null) {
                try {
                    if (transactionStatus.isNewTransaction()) {
                        transactionManager.commit(transactionStatus);
                    }
                } catch (Exception ex) {
                    throw new IllegalStateException("Unable to commit transaction", ex);
                }
            }
        }
    }
}
