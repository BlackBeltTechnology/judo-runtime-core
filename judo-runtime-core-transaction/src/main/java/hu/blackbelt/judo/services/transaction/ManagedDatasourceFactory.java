package hu.blackbelt.judo.services.transaction;

import javax.sql.DataSource;

public interface ManagedDatasourceFactory {
   DataSource create(DataSource dataSource, String name);
}
