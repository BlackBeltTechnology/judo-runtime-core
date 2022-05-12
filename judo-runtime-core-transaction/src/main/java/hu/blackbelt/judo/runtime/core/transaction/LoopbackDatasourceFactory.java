package hu.blackbelt.judo.runtime.core.transaction;

import javax.sql.DataSource;

public class LoopbackDatasourceFactory implements ManagedDatasourceFactory {
    public DataSource create(DataSource dataSource, String name) {
        return dataSource;
    }
}
