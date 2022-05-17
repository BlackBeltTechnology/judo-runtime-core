package hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql;

import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;

public class PostgresqlDialect implements Dialect {
    @Override
    public String getName() {
        return "postgresql";
    }

    @Override
    public String getDualTable() {
        return null;
    }
}
