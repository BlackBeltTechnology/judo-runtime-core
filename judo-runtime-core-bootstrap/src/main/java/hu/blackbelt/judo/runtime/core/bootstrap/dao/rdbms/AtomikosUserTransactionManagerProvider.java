package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.atomikos.icatch.jta.UserTransactionManager;
import com.google.inject.Provider;

import javax.transaction.TransactionManager;

public class AtomikosUserTransactionManagerProvider implements Provider<TransactionManager> {
    @Override
    public TransactionManager get() {
        return new UserTransactionManager();
    }
}
