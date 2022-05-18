package hu.blackbelt.judo.runtime.core.dao.rdbms.fixture;


import com.atomikos.icatch.jta.UserTransactionManager;
import com.atomikos.jdbc.AtomikosNonXADataSourceBean;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import javax.transaction.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Supplier;

@Slf4j
public class JudoDatasourceFixture {

//    static {
//        System.setProperty("user.timezone", "UTC");
//        //         TZ: 'GMT+2'
//        //        PGTZ: 'GMT+2'
//    }

    public static final String CONTAINER_NONE = "none";
    public static final String CONTAINER_POSTGRESQL = "postgresql";
    public static final String CONTAINER_YUGABYTEDB = "yugabytedb";

    public static final String DIALECT_HSQLDB = "hsqldb";
    public static final String DIALECT_POSTGRESQL = "postgresql";

    @Getter
    protected String dialect = System.getProperty("dialect", DIALECT_HSQLDB);

    @Getter
    protected String container = System.getProperty("container", CONTAINER_NONE);

    @Getter
    protected DataSource dataSource;


    @Getter
    TransactionManager transactionManager;

    @SuppressWarnings("rawtypes")
	public JdbcDatabaseContainer sqlContainer;

    @SuppressWarnings({ "rawtypes", "resource" })
	public void setupDatabase() {
        if (dialect.equals(DIALECT_POSTGRESQL)) {
            if (container.equals(CONTAINER_NONE) || container.equals(CONTAINER_POSTGRESQL)) {
                sqlContainer =
                        (PostgreSQLContainer) new PostgreSQLContainer("postgres:latest").withStartupTimeout(Duration.ofSeconds(600));
//                                .withEnv("TZ", "GMT")
//                                .withEnv("PGTZ", "GMT");
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

    public void dropSchema() {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            if (dialect.equals(DIALECT_POSTGRESQL)) {
                statement.execute("select 'drop table \"' || tablename || '\" cascade;' from pg_tables;");
            } else if (dialect.equals(DIALECT_HSQLDB)) {
                statement.execute("TRUNCATE SCHEMA PUBLIC RESTART IDENTITY AND COMMIT NO CHECK");
                //statement.execute("DROP SCHEMA PUBLIC CASCADE");
            }
        } catch (SQLException throwables) {
            throw new RuntimeException("Could not drop schema", throwables);
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
        } else if (dialect.equals(DIALECT_POSTGRESQL)) {
            sqlContainer.start();
            ds.setDriverClassName(sqlContainer.getDriverClassName());
            ds.setUrl(sqlContainer.getJdbcUrl());
            ds.setUser(sqlContainer.getUsername());
            ds.setPassword(sqlContainer.getPassword());
        } else {
            throw new IllegalStateException("Unsupported dialect: " + dialect);
        }
        dataSource = ds;


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
