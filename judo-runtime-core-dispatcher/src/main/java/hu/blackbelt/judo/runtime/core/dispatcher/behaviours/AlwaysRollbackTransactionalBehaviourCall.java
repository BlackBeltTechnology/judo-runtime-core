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

import javax.transaction.Status;
import javax.transaction.TransactionManager;
import java.util.Map;

@Slf4j
public abstract class AlwaysRollbackTransactionalBehaviourCall<ID> implements BehaviourCall<ID> {

    TransactionManager transactionManager;
    Context context;

    private static final String ROLLBACK_KEY = "ROLLBACK";

    public AlwaysRollbackTransactionalBehaviourCall(Context context, TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        this.context = context;
    }

    public abstract Object callInRollbackTransaction(Map<String, Object> exchange, EOperation operation);

    @SneakyThrows
    @Override
    public Object call(Map<String, Object> exchange, EOperation operation) {

        boolean transactionContext = false;

        Object rollback = context.get(ROLLBACK_KEY);
        if (transactionManager != null) {
            if (transactionManager.getStatus() == Status.STATUS_NO_TRANSACTION) {
                transactionManager.begin();
                transactionContext = true;
            }
        } else {
            log.warn("No transaction manager is registered, operation will NOT rollback.");
        }
        context.put(ROLLBACK_KEY, Boolean.TRUE);
        try {
            return callInRollbackTransaction(exchange, operation);
        } finally {
            context.put(ROLLBACK_KEY, rollback);
            if (transactionContext) {
                try {
                    transactionManager.rollback();
                } catch (Exception ex) {
                    throw new IllegalStateException("Unable to rollback transaction", ex);
                }
            }
        }

    }
}
