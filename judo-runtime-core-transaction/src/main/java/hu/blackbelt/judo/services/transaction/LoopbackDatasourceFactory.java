package hu.blackbelt.judo.services.transaction;

import lombok.extern.slf4j.Slf4j;
import javax.sql.DataSource;

@Slf4j
public class LoopbackDatasourceFactory implements ManagedDatasourceFactory {
    public DataSource create(DataSource dataSource, String name) {
        return dataSource;
    }
}
