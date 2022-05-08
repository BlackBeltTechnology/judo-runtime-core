package hu.blackbelt.judo.runtime.core.dao.rdbms.fixture;


import com.atomikos.icatch.jta.UserTransactionManager;
import com.atomikos.jdbc.AtomikosNonXADataSourceBean;
import hu.blackbelt.judo.runtime.core.persitence.postgresql.Marker;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.core.HsqlDatabase;
import liquibase.database.core.PostgresDatabase;
import liquibase.database.jvm.HsqlConnection;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.ttddyy.dsproxy.listener.logging.DefaultQueryLogEntryCreator;
import net.ttddyy.dsproxy.listener.logging.SLF4JQueryLoggingListener;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamCastMode;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import javax.transaction.*;
import java.sql.Connection;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Supplier;

@Slf4j
public class RdbmsDatasourceFixture {

    public static final String CONTAINER_NONE = "none";
    public static final String CONTAINER_POSTGRESQL = "postgresql";
    public static final String CONTAINER_YUGABYTEDB = "yugabytedb";

    public static final String DIALECT_HSQLDB = "hsqldb";
    public static final String DIALECT_POSTGRESQL = "postgresql";
    public static final String DIALECT_ORACLE = "oracle";

    @Getter
    protected String dialect = System.getProperty("dialect", DIALECT_HSQLDB);

    @Getter
    protected boolean jooqEnabled = Boolean.parseBoolean(System.getProperty("jooqEnabled"));

    @Getter
    protected String container = System.getProperty("container", CONTAINER_NONE);

    @Getter
    protected DataSource originalDataSource;

    @Getter
    protected DataSource wrappedDataSource;

    @Getter
    protected DSLContext jooqContext;

    @Getter
    protected Database liquibaseDb;

    @Getter
    TransactionManager transactionManager;

    public JdbcDatabaseContainer sqlContainer;

    public void setupDatasource() {
        if (dialect.equals(DIALECT_POSTGRESQL)) {
            if (container.equals(CONTAINER_NONE) || container.equals(CONTAINER_POSTGRESQL)) {
                sqlContainer =
                        (PostgreSQLContainer) new PostgreSQLContainer().withStartupTimeout(Duration.ofSeconds(600));
            } else if (container.equals(CONTAINER_YUGABYTEDB)) {
                sqlContainer =
                        (YugabytedbSQLContainer) new YugabytedbSQLContainer().withStartupTimeout(Duration.ofSeconds(600));
            }
        }
    }

    public void teardownDatasource() throws Exception {
        if (sqlContainer != null && sqlContainer.isRunning()) {
            sqlContainer.stop();
        }
    }


    @SneakyThrows
    public void prepareDatasources() {
        transactionManager = new UserTransactionManager();

        System.setProperty("com.atomikos.icatch.registered", "true");
        UUID uuid = UUID.randomUUID();
        System.setProperty("com.atomikos.icatch.output_dir", "target/test-classes/tm/data/logs/" + uuid  + "/ ");
        System.setProperty("com.atomikos.icatch.log_base_dir", "target/test-classes/tm/data/logs/"  + uuid + "/");
        AtomikosNonXADataSourceBean ds = new AtomikosNonXADataSourceBean();
        ds.setPoolSize(10);
        ds.setLocalTransactionMode(true);
        ds.setUniqueResourceName("db");


        if (dialect.equals(DIALECT_HSQLDB)) {
            ds.setUniqueResourceName("hsqldb");
            ds.setDriverClassName("org.hsqldb.jdbcDriver");
            ds.setUrl("jdbc:hsqldb:mem:memdb");
            ds.setUser("sa");
            ds.setPassword("saPassword");
            liquibaseDb = new HsqlDatabase();
        } else if (dialect.equals(DIALECT_POSTGRESQL)) {
            sqlContainer.start();
            ds.setDriverClassName(sqlContainer.getDriverClassName());
            ds.setUrl(sqlContainer.getJdbcUrl());
            ds.setUser(sqlContainer.getUsername());
            ds.setPassword(sqlContainer.getPassword());
            liquibaseDb = new PostgresDatabase();
        } else {
            throw new IllegalStateException("Unsupported dialect: " + dialect);
        }

        SLF4JQueryLoggingListener loggingListener = new SLF4JQueryLoggingListener();
        loggingListener.setQueryLogEntryCreator(new DefaultQueryLogEntryCreator());

//        dataSource = ProxyDataSourceBuilder
//                .create(ds)
//                .name("DATA_SOURCE_PROXY")
//                .listener(loggingListener)
//                .build();

        originalDataSource = ds;

        // Execute dialect based datatsource preprations
        if (dialect.equals(DIALECT_POSTGRESQL)) {
            executeInitiLiquibase(Marker.class.getClassLoader(), "liquibase/postgresql-init-changelog.xml", ds);
        }

        wrappedDataSource = ProxyDataSourceBuilder
                .create(jooqEnabled ? getJooqContext(ds).parsingDataSource() : ds)
                .name("JOOQ_DATA_SOURCE_PROXY")
                .listener(loggingListener)
                .build();
    }

    private SQLDialect getJooqSqlDialect() {
        if (dialect.equals(DIALECT_HSQLDB)) {
            return SQLDialect.HSQLDB;
        } else if (dialect.equals(DIALECT_POSTGRESQL)) {
            return SQLDialect.POSTGRES;
        } else {
            throw new IllegalStateException("Unsupported dialect: " + dialect);
        }
    }

    private DSLContext getJooqContext(DataSource dataSource) {
        if (jooqContext == null) {
            final Settings settings = new Settings();
            settings.setParamCastMode(ParamCastMode.NEVER);
            jooqContext = DSL.using(dataSource, getJooqSqlDialect(), settings);
        }
        return jooqContext;
    }

    @SneakyThrows
    public void setLiquibaseDbDialect(Connection connection) {
        if (DIALECT_HSQLDB.equals(dialect)) {
            liquibaseDb.setConnection(new HsqlConnection(connection));
        } else {
            liquibaseDb.setConnection(new JdbcConnection(connection));
        }
        liquibaseDb.setAutoCommit(false);
    }

    public void executeInitiLiquibase(ClassLoader classLoader, String name, DataSource dataSource) {
        try {
            setLiquibaseDbDialect(dataSource.getConnection());
            final Liquibase liquibase = new Liquibase(name,
                    new ClassLoaderResourceAccessor(classLoader), liquibaseDb);
            liquibase.update((String) null);
            liquibaseDb.close();
        } catch (Exception e) {
            log.error("Error init liquibase", e);
            throw new RuntimeException(e);
        }
    }

    public <T extends Throwable> T assertThrowsInTransaction(final Class<T> expectedType, final Executable executable) {
            return Assertions.assertThrows(expectedType, () -> {
                getTransactionManager().begin();
                try {
                    executable.execute();
                } catch (Exception e) {
                    if (getTransactionManager().getStatus() == Status.STATUS_ACTIVE) {
                        getTransactionManager().rollback();
                    }
                } finally {
                    if (getTransactionManager().getStatus() == Status.STATUS_ACTIVE) {
                        getTransactionManager().commit();
                    }
                }
            });
    }

    @SneakyThrows
    public <R> R runInTransaction(Supplier<R> executable) {
        getTransactionManager().begin();
        try {
            return executable.get();
        } catch (Exception e) {
            if (getTransactionManager().getStatus() == Status.STATUS_ACTIVE) {
                getTransactionManager().rollback();
            }
        } finally {
            if (getTransactionManager().getStatus() == Status.STATUS_ACTIVE) {
                getTransactionManager().commit();
            }
        }
        return null;
    }

}
