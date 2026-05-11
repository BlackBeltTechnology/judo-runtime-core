package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.SimpleLiquibaseExecutor;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only delegating wrapper around {@link SimpleLiquibaseExecutor} that counts
 * how many times the schema-creating entry point ({@link #createDatabase}) has been
 * invoked. Used by regression tests in the {@code cache-byclass-test-runtime}
 * change to verify that Liquibase runs at most once per cached runtime in
 * {@code BY_CLASS} / {@code SINGLETON} modes.
 *
 * <p>Package-private intentionally: this is an internal testkit seam, not a
 * public API.
 */
final class CountingLiquibaseExecutor extends SimpleLiquibaseExecutor {

    private final AtomicInteger executionCount = new AtomicInteger(0);

    /**
     * @return the number of times {@link #createDatabase(DataSource, LiquibaseModel)} has been invoked.
     */
    int executionCount() {
        return executionCount.get();
    }

    @Override
    public void createDatabase(DataSource dataSource, LiquibaseModel liquibaseModel) {
        executionCount.incrementAndGet();
        super.createDatabase(dataSource, liquibaseModel);
    }
}
