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

import lombok.SneakyThrows;
import org.eclipse.emf.ecore.EOperation;

import javax.transaction.Status;
import javax.transaction.TransactionManager;
import java.util.Map;

public abstract class TransactionalBehaviourCall<ID> implements BehaviourCall<ID> {

    TransactionManager transactionManager;

    public  TransactionalBehaviourCall(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public abstract Object callInTransaction(Map<String, Object> exchange, EOperation operation);

    @SneakyThrows
    @Override
    public Object call(Map<String, Object> exchange, EOperation operation) {
        boolean transactionContext = false;

        if (transactionManager != null) {
            if (transactionManager.getStatus() == Status.STATUS_NO_TRANSACTION) {
                transactionManager.begin();
                transactionContext = true;
            }
        }
        try {
            return callInTransaction(exchange, operation);
        } catch (Exception e) {
            if (transactionContext) {
                try {
                    transactionManager.rollback();
                } catch (Exception ex) {
                    throw new IllegalArgumentException("Unable to rollback transaction", ex);
                } finally {
                    transactionContext = false;
                }
            }
            throw e;
        } finally {
            if (transactionContext) {
                try {
                    transactionManager.commit();
                } catch (Exception ex) {
                    throw new IllegalStateException("Unable to commit transaction", ex);
                }
            }
        }
    }
}
