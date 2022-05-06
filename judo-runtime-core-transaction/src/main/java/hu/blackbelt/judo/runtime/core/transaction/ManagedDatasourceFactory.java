package hu.blackbelt.judo.runtime.core.transaction;

import javax.sql.DataSource;

public interface ManagedDatasourceFactory {
   DataSource create(DataSource dataSource, String name);
}
