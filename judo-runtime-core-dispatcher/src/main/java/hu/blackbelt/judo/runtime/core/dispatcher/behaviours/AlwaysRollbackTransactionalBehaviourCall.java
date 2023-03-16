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
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EOperation;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.util.Map;

@Slf4j
public abstract class AlwaysRollbackTransactionalBehaviourCall<ID> implements BehaviourCall<ID> {
    private static final String ROLLBACK_KEY = "ROLLBACK";

    PlatformTransactionManager transactionManager;
    Context context;

    public AlwaysRollbackTransactionalBehaviourCall(Context context, PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        this.context = context;
    }

    public abstract Object callInRollbackTransaction(Map<String, Object> exchange, EOperation operation);

    @SneakyThrows
    @Override
    public Object call(Map<String, Object> exchange, EOperation operation) {

        TransactionStatus transactionStatus = null;
        if (transactionManager != null) {
            DefaultTransactionDefinition transactionDefinition = new DefaultTransactionDefinition();
            //transactionDefinition.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
            transactionStatus = transactionManager.getTransaction(transactionDefinition);
        }
        if (transactionStatus.isNewTransaction()) {
            context.put(ROLLBACK_KEY, Boolean.TRUE);
        }
        try {
            return callInRollbackTransaction(exchange, operation);
        } finally {
            if (transactionStatus != null) {
                try {
                    if (transactionStatus.isNewTransaction()) {
                        context.put(ROLLBACK_KEY, Boolean.FALSE);
                        transactionManager.rollback(transactionStatus);
                    }
                } catch (Exception ex) {
                    throw new IllegalStateException("Unable to rollback transaction", ex);
                }
            }
        }
    }
}
