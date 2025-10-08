package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import com.google.common.collect.ImmutableMap;
import com.zaxxer.hikari.HikariDataSource;
import hu.blackbelt.judo.meta.rdbms.RdbmsTable;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsUtils;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.eclipse.emf.common.util.BasicEList;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class JudoDatasourceFixture {

    private static final Logger log = LoggerFactory.getLogger(
        JudoDatasourceFixture.class
    );

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
    public static final Map<String, String> STATMENT_TEMPLATE = ImmutableMap.of(
        DIALECT_POSTGRESQL,
        "TRUNCATE TABLE %s RESTART IDENTITY CASCADE;",
        DIALECT_HSQLDB,
        "TRUNCATE TABLE %s RESTART IDENTITY AND COMMIT NO CHECK"
    );

    protected String dialect = System.getProperty("dialect", DIALECT_HSQLDB);

    protected String container = System.getProperty(
        "container",
        CONTAINER_NONE
    );

    protected String timezone = System.getProperty("tz", "GMT");

    protected DataSource dataSource;

    PlatformTransactionManager transactionManager;

    public JdbcDatabaseContainer sqlContainer;

    public String getDialect() {
        return dialect;
    }

    public String getContainer() {
        return container;
    }

    public String getTimezone() {
        return timezone;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public PlatformTransactionManager getTransactionManager() {
        return transactionManager;
    }

    public JdbcDatabaseContainer getSqlContainer() {
        return sqlContainer;
    }

    public void setContainer(String container) {
        this.container = container;
    }

    public void setDialect(String dialect) {
        this.dialect = dialect;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public void setupDatabase() {
        if (dialect.equals(DIALECT_POSTGRESQL)) {
            if (
                container.equals(CONTAINER_NONE) ||
                container.equals(CONTAINER_POSTGRESQL)
            ) {
                sqlContainer = (PostgreSQLContainer) new PostgreSQLContainer(
                    "postgres:latest"
                )
                    .withStartupTimeout(Duration.ofSeconds(600))
                    .withEnv("TZ", timezone)
                    .withEnv("PGTZ", timezone);
            } else if (container.equals(CONTAINER_YUGABYTEDB)) {
                sqlContainer =
                    (YugabytedbSQLContainer) new YugabytedbSQLContainer()
                        .withStartupTimeout(Duration.ofSeconds(600))
                        .withEnv("TZ", timezone)
                        .withEnv("PGTZ", timezone);
            }
        }
    }

    public void teardownDatasource() {
        if (sqlContainer != null && sqlContainer.isRunning()) {
            sqlContainer.stop();
        }
    }

    public void truncateTables(RdbmsModel rdbmsModel) {
        RdbmsUtils rdbmsUtils = new RdbmsUtils(rdbmsModel.getResourceSet());
        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement()
        ) {
            for (RdbmsTable rdbmsTable : rdbmsUtils
                .getRdbmsTables()
                .orElse(new BasicEList<>())) {
                log.debug(
                    "Truncating table: %s (%s)".formatted(
                        rdbmsTable.getName(),
                        rdbmsTable.getSqlName()
                    )
                );
                statement.execute(
                    STATMENT_TEMPLATE.get(dialect).formatted(
                        rdbmsTable.getSqlName()
                    )
                );
            }
        } catch (SQLException throwables) {
            throw new RuntimeException("Could not truncate tables", throwables);
        }
    }

    public void dropTables(RdbmsModel rdbmsModel) {
        RdbmsUtils rdbmsUtils = new RdbmsUtils(rdbmsModel.getResourceSet());
        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement()
        ) {
            for (RdbmsTable rdbmsTable : rdbmsUtils
                .getRdbmsTables()
                .orElse(new BasicEList<>())) {
                log.debug(
                    "Drop table: %s (%s)".formatted(
                        rdbmsTable.getName(),
                        rdbmsTable.getSqlName()
                    )
                );
                statement.execute(
                    "DROP TABLE %s CASCADE;".formatted(rdbmsTable.getSqlName())
                );
            }
        } catch (SQLException throwables) {
            throw new RuntimeException("Could not drop tables", throwables);
        }
    }

    public void prepareDatasources() {
        if (dialect.equals(DIALECT_HSQLDB)) {
            final JDBCDataSource ds = new JDBCDataSource();
            ds.setUrl("jdbc:hsqldb:mem:" + UUID.randomUUID());
            ds.setUser("sa");
            ds.setPassword("saPassword");
            dataSource = ds;
        } else if (dialect.equals(DIALECT_POSTGRESQL)) {
            sqlContainer.start();
            final PGSimpleDataSource ds = new PGSimpleDataSource();
            ds.setURL(sqlContainer.getJdbcUrl());
            ds.setUser(sqlContainer.getUsername());
            ds.setPassword(sqlContainer.getPassword());
            dataSource = ds;
        } else {
            throw new IllegalStateException("Unsupported dialect: " + dialect);
        }

        // Initialize transaction manager for the datasource
        transactionManager = new DataSourceTransactionManager(dataSource);
    }

    public <T extends Throwable> T assertThrowsInTransaction(
        final Class<T> expectedType,
        final Executable executable
    ) {
        return Assertions.assertThrows(expectedType, () -> {
            TransactionStatus transactionStatus =
                getTransactionManager().getTransaction(
                    new DefaultTransactionDefinition()
                );
            try {
                executable.execute();
            } catch (Exception e) {
                if (!transactionStatus.isCompleted()) {
                    getTransactionManager().rollback(transactionStatus);
                }
            } finally {
                if (!transactionStatus.isCompleted()) {
                    getTransactionManager().commit(transactionStatus);
                }
            }
        });
    }

    public <R> R runInTransaction(Supplier<R> executable) {
        TransactionStatus transactionStatus =
            getTransactionManager().getTransaction(
                new DefaultTransactionDefinition()
            );
        try {
            return executable.get();
        } catch (Exception e) {
            if (!transactionStatus.isCompleted()) {
                getTransactionManager().rollback(transactionStatus);
            }
        } finally {
            if (!transactionStatus.isCompleted()) {
                getTransactionManager().commit(transactionStatus);
            }
        }
        return null;
    }
}
