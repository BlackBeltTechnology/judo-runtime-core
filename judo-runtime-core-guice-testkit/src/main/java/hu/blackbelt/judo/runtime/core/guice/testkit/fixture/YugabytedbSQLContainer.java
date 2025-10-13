package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import static java.time.temporal.ChronoUnit.SECONDS;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

/**
 * Testcontainers implementation for YugabyteDB.
 * <p>
 * YugabyteDB is a distributed SQL database that is PostgreSQL-compatible.
 * This container uses the Yugabyted single-node cluster process and exposes
 * the YSQL (Yugabyte SQL) API on port 5433.
 * </p>
 * <p>
 * Configuration is done via YSQL_* environment variables (not POSTGRES_* variables):
 * <ul>
 *   <li>YSQL_DB - database name</li>
 *   <li>YSQL_USER - username</li>
 *   <li>YSQL_PASSWORD - password</li>
 * </ul>
 * </p>
 * <p>
 * Example usage:
 * <pre>
 * YugabytedbSQLContainer container = new YugabytedbSQLContainer()
 *     .withDatabaseName("testdb")
 *     .withUsername("testuser")
 *     .withPassword("testpass");
 * container.start();
 * </pre>
 * </p>
 */
public class YugabytedbSQLContainer
    extends JdbcDatabaseContainer<YugabytedbSQLContainer> {

    public static final String IMAGE = "yugabytedb/yugabyte";
    public static final String DEFAULT_TAG = "2.1.8.2-b1";

    public static final Integer YUGABYTE_PORT = 5433;

    static final String DEFAULT_USER = "yugabyte";

    static final String DEFAULT_PASSWORD = "yugabyte";

    private String databaseName = "yugabyte";
    private String username = "yugabyte";
    private String password = "yugabyte";

    //private static final String FSYNC_OFF_OPTION = "fsync=off";

    private static final String QUERY_PARAM_SEPARATOR = "&";

    public YugabytedbSQLContainer() {
        this(IMAGE + ":" + DEFAULT_TAG);
    }

    public YugabytedbSQLContainer(final String dockerImageName) {
        super(DockerImageName.parse(dockerImageName));
        this.waitStrategy = new LogMessageWaitStrategy()
            .withRegEx(".*yugabyted started successfully.*")
            .withTimes(1)
            .withStartupTimeout(Duration.of(60, SECONDS));
        //this.setCommand("postgres", "-c", FSYNC_OFF_OPTION);
        this.setCommand(
            "/bin/bash",
            "-c",
            "bin/yugabyted start --ui=false && tail -f /dev/null"
        );
        addExposedPort(YUGABYTE_PORT);
    }

    @Override
    public Set<Integer> getLivenessCheckPortNumbers() {
        return new HashSet<>(getMappedPort(YUGABYTE_PORT));
    }

    @Override
    protected void configure() {
        // Disable Postgres driver use of java.util.logging to reduce noise at startup time
        withUrlParam("loggerLevel", "OFF");
        // Configure Yugabyted YSQL (Yugabyte SQL) environment variables
        addEnv("YSQL_DB", databaseName);
        addEnv("YSQL_USER", username);
        addEnv("YSQL_PASSWORD", password);
    }

    @Override
    public String getDriverClassName() {
        return "org.postgresql.Driver";
    }

    @Override
    public String getJdbcUrl() {
        String additionalUrlParams = constructUrlParameters(
            "?",
            QUERY_PARAM_SEPARATOR
        );
        return (
            "jdbc:postgresql://" +
            getHost() +
            ":" +
            getMappedPort(YUGABYTE_PORT) +
            "/" +
            databaseName +
            additionalUrlParams
        );
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getTestQueryString() {
        return "SELECT 1";
    }

    @Override
    public YugabytedbSQLContainer withDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
        return self();
    }

    @Override
    public YugabytedbSQLContainer withUsername(final String username) {
        this.username = username;
        return self();
    }

    @Override
    public YugabytedbSQLContainer withPassword(final String password) {
        this.password = password;
        return self();
    }

    @Override
    protected void waitUntilContainerStarted() {
        getWaitStrategy().waitUntilReady(this);
    }
}
