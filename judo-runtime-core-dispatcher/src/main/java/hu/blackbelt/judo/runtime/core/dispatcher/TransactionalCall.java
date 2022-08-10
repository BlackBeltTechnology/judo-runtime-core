package hu.blackbelt.judo.runtime.core.dispatcher;

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

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EOperation;

import javax.transaction.Status;
import javax.transaction.TransactionManager;
import java.util.function.Function;

@AllArgsConstructor
@Slf4j
public class TransactionalCall implements Function<Payload, Payload> {

    final TransactionManager transactionManager;
    final Function<Payload, Payload> functionToCall;
    final EOperation operation;

    @SneakyThrows
    @Override
    public Payload apply(Payload payload) {
        boolean transactionContext = false;

        if (transactionManager != null) {
            if (transactionManager.getStatus() == Status.STATUS_NO_TRANSACTION) {
                transactionManager.begin();
                transactionContext = true;
            }
        }
        try {
            return functionToCall.apply(payload);
        } catch (Exception e) {
            if (transactionContext) {
                transactionContext = false;
                try {
                    transactionManager.rollback();
                } catch (Exception ex) {
                    log.error("Unable to rollback transaction");
                }
            }
            throw e;
        } finally {
            if (transactionContext ) {
                if (AsmUtils.isStateful(operation)) {
                    transactionManager.commit();
                } else {
                    transactionManager.rollback();
                }
            }
        }
    }
}
