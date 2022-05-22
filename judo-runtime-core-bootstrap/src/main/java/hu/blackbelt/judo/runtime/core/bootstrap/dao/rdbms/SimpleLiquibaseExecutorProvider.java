package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms;

import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.SimpleLiquibaseExecutor;

public class SimpleLiquibaseExecutorProvider implements Provider<SimpleLiquibaseExecutor> {
    @Override
    public SimpleLiquibaseExecutor get() {
        return new SimpleLiquibaseExecutor();
    }
}
