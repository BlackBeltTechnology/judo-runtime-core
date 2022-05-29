package hu.blackbelt.judo.runtime.core.spring;

import hu.blackbelt.judo.runtime.core.bootstrap.JudoDefaultModule;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelHolder;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb.JudoHsqldbDatasourceWrapperModule;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb.JudoHsqldbModules;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.guice.annotation.EnableGuiceModules;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

@Configuration
@EnableGuiceModules
public class JudoJdbcSpringConfiguration {

    @Bean
    public static JudoHsqldbDatasourceWrapperModule judoHsqldbModules(DataSource datasource, TransactionManager transactionManager) {
        return new JudoHsqldbDatasourceWrapperModule(datasource, transactionManager);
        //return JudoHsqldbModules.builder().build();
        // return new JudoHsqldbModules.JudoHsqldbModulesBuilder().build();
    }

}
