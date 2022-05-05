package hu.blackbelt.judo.services.dispatcher;

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
