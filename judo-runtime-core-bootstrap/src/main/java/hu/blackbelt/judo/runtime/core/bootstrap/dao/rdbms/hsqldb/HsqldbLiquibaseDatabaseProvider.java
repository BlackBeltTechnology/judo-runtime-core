package hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb;

import com.google.inject.Provider;
import liquibase.database.Database;
import liquibase.database.core.HsqlDatabase;

public class HsqldbLiquibaseDatabaseProvider implements Provider<Database> {
    @Override
    public Database get() {
        return new HsqlDatabase();
    }
}
