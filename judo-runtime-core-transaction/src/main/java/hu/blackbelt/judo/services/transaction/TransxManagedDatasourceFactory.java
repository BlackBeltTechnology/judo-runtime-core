package hu.blackbelt.judo.services.transaction;

import lombok.extern.slf4j.Slf4j;
import org.ops4j.pax.transx.jdbc.ManagedDataSourceBuilder;
import org.osgi.service.component.annotations.*;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.AttributeType;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import javax.resource.spi.TransactionSupport;
import javax.sql.DataSource;

@Component(configurationPolicy = ConfigurationPolicy.REQUIRE)
@Slf4j
public class TransxManagedDatasourceFactory implements ManagedDatasourceFactory {

    @ObjectClassDefinition
    public @interface Config {

        @AttributeDefinition(name = "Transx provider type", description = "Do not set", type = AttributeType.STRING)
        String provider() default "transx";

        @AttributeDefinition(name = "XA Pooled Datasource", description = "When cheked the DataSource is XA transaction pooled", type = AttributeType.BOOLEAN)
        boolean xa() default false;

        @AttributeDefinition(name = "Alive bypass window in millisecond", description = "Alive bypass window in millisecond", type = AttributeType.LONG)
        long aliveBypassWindow() default 500L;

        @AttributeDefinition(name = "Commit before autocommit", description = "Commit before autocommit", type = AttributeType.BOOLEAN)
        boolean commitBeforeAutocommit() default false;

        @AttributeDefinition(name = "Connection timeout in millisecond", description = "Connection timeout in millisecond", type = AttributeType.LONG)
        long connectionTimeout() default 30L * 1000L;

        @AttributeDefinition(name = "House keeping period in millisecond", description = "House keeping period in millisecond", type = AttributeType.LONG)
        long houseKeepingPeriod() default 30L * 1000L;

        @AttributeDefinition(name = "Idle timeout in millisecond", description = "Idle timeout in millisecond", type = AttributeType.LONG)
        long idleTimeout() default 10L * 60L * 1000L;

        @AttributeDefinition(name = "Maximum lifetime in millisecond", description = "Maximum lifetime in millisecond", type = AttributeType.LONG)
        long maxLifetime() default 30L * 60L * 1000L;

        @AttributeDefinition(name = "Maximum pool size", description = "When -1 is set it will be the same as minIdle or 10", type = AttributeType.INTEGER)
        int maxPoolSize() default -1;

        @AttributeDefinition(name = "Minimum idle size", description = "When -1 is set it will be the same as minIdle or 10", type = AttributeType.INTEGER)
        int minIdle() default -1;

        @AttributeDefinition(name = "Prepared statement cache size", description = "Prepared statement cache size", type = AttributeType.INTEGER)
        int preparedStatementCacheSize() default 0;
    }

    @Reference(policyOption = ReferencePolicyOption.GREEDY)
    private org.ops4j.pax.transx.tm.TransactionManager transxTransactionManager;

    TransactionSupport.TransactionSupportLevel transactionSupportLevel = TransactionSupport.TransactionSupportLevel.LocalTransaction;

    Long aliveBypassWindow;
    Boolean commitBeforeAutocommit;
    Long connectionTimeout;
    Long houseKeepingPeriod;
    Long idleTimeout;
    Long maxLifetime;
    Integer maxPoolSize;
    Integer minIdle;
    Integer preparedStatementCacheSize;

    public void activate(Config config) {
        if (config.xa()) {
            transactionSupportLevel = TransactionSupport.TransactionSupportLevel.XATransaction;
        }

        aliveBypassWindow = config.aliveBypassWindow();
        commitBeforeAutocommit = config.commitBeforeAutocommit();
        connectionTimeout = config.connectionTimeout();
        houseKeepingPeriod = config.houseKeepingPeriod();
        idleTimeout = config.idleTimeout();
        maxLifetime = config.maxLifetime();
        maxPoolSize = config.maxPoolSize();
        minIdle = config.minIdle();
        preparedStatementCacheSize = config.preparedStatementCacheSize();
    }

    public DataSource create(DataSource dataSource, String name) {
        DataSource managedDs = null;
        if (transxTransactionManager != null) {
            try {
                managedDs = ManagedDataSourceBuilder.builder()
                        .name(name + "-transx")
                        .dataSource(dataSource)
                        .transactionManager(transxTransactionManager)
                        .transaction(transactionSupportLevel)
                        .aliveBypassWindow(aliveBypassWindow)
                        .commitBeforeAutocommit(commitBeforeAutocommit)
                        .connectionTimeout(connectionTimeout)
                        .houseKeepingPeriod(houseKeepingPeriod)
                        .idleTimeout(idleTimeout)
                        .maxLifetime(maxLifetime)
                        .maxPoolSize(maxPoolSize)
                        .minIdle(minIdle)
                        .preparedStatementCacheSize(preparedStatementCacheSize)
                        .build();
            } catch (Exception e) {
                log.error("Could not create transaction managed dataSource for " + name);
            }
        } else {
            managedDs = dataSource;
        }
        return managedDs;
    }
}
