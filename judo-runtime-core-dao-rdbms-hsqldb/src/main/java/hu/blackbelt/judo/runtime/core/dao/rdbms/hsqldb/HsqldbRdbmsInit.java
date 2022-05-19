package hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb;

import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsInit;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.SimpleLiquibaseExecutor;
import lombok.Builder;
import lombok.NonNull;

import javax.sql.DataSource;

public class HsqldbRdbmsInit implements RdbmsInit {
    private final SimpleLiquibaseExecutor liquibaseExecutor;

    private final LiquibaseModel liquibaseModel;

    @Builder
    public HsqldbRdbmsInit(@NonNull SimpleLiquibaseExecutor liquibaseExecutor, @NonNull LiquibaseModel liquibaseModel) {
        this.liquibaseExecutor = liquibaseExecutor;
        this.liquibaseModel = liquibaseModel;
    }

    public void execute(DataSource dataSource) {
        liquibaseExecutor.createDatabase(dataSource, liquibaseModel);
    }
}
