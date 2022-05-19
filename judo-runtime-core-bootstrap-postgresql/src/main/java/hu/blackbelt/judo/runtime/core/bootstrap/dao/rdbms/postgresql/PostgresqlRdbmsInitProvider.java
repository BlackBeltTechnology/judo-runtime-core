package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.postgresql;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelHolder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.SimpleLiquibaseExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.PostgresqlRdbmsInit;

import javax.sql.DataSource;

public class PostgresqlRdbmsInitProvider implements Provider<RdbmsInit> {

    @Inject
    SimpleLiquibaseExecutor simpleLiquibaseExecutor;

    @Inject
    DataSource dataSource;

    @Inject
    JudoModelHolder models;

    @Override
    public RdbmsInit get() {
        PostgresqlRdbmsInit init = PostgresqlRdbmsInit.builder()
                .liquibaseExecutor(simpleLiquibaseExecutor)
                .liquibaseModel(models.getLiquibaseModel())
                .build();
        init.execute(dataSource);
        return init;
    }
}
