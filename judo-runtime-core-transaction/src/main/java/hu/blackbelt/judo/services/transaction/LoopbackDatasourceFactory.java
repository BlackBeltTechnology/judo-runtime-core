package hu.blackbelt.judo.services.transaction;

import lombok.extern.slf4j.Slf4j;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import javax.sql.DataSource;

@Component(configurationPolicy = ConfigurationPolicy.REQUIRE, property = "provider=loopback")
@Slf4j
public class LoopbackDatasourceFactory implements ManagedDatasourceFactory {
    public DataSource create(DataSource dataSource, String name) {
        return dataSource;
    }
}
