package hu.blackbelt.judo.services.dispatcher.behaviours;

import lombok.SneakyThrows;
import org.eclipse.emf.ecore.EOperation;

import javax.transaction.Status;
import javax.transaction.TransactionManager;
import java.util.Map;

public abstract class TransactionalBehaviourCall implements BehaviourCall {

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
