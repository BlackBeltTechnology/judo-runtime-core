package hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb;

import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;

public class HsqldbDialect implements Dialect {
    @Override
    public String getDualTable() {
        return "\"INFORMATION_SCHEMA\".\"SYSTEM_USERS\"";
    }
}
