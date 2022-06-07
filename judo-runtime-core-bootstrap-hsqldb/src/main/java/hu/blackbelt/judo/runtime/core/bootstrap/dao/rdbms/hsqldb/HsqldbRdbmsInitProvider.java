package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbRdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.SimpleLiquibaseExecutor;

import javax.sql.DataSource;

public class HsqldbRdbmsInitProvider implements Provider<RdbmsInit> {

    @Inject
    SimpleLiquibaseExecutor simpleLiquibaseExecutor;

    @Inject
    DataSource dataSource;

    @Inject
    JudoModelLoader models;

    @Override
    public RdbmsInit get() {
        HsqldbRdbmsInit init = HsqldbRdbmsInit.builder()
                .liquibaseExecutor(simpleLiquibaseExecutor)
                .liquibaseModel(models.getLiquibaseModel())
                .build();
        init.execute(dataSource);
        return init;
    }
}
