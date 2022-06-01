package hu.blackbelt.judo.runtime.core.spring;

import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.ConfigurationCondition;

import javax.sql.DataSource;

public class JudoDataSourceCondition extends AnyNestedCondition {

    JudoDataSourceCondition() {
        super(ConfigurationPhase.REGISTER_BEAN);
    }

    @ConditionalOnBean(DataSource.class)
    private static final class DataSourceBeanCondition {
    }

//        @ConditionalOnProperty(prefix = "spring.liquibase", name = "url")
//        private static final class LiquibaseUrlCondition {
//        }
}
