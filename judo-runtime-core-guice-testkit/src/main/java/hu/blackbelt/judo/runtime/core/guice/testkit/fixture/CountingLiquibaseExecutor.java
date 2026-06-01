package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.SimpleLiquibaseExecutor;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only delegating wrapper around {@link SimpleLiquibaseExecutor} that counts
 * how many times the schema-creating entry point ({@link #createDatabase}) has
 * <b>successfully</b> completed. Used by regression tests in the
 * {@code cache-byclass-test-runtime} change to verify that Liquibase runs at
 * most once per cached runtime in {@code BY_CLASS} / {@code SINGLETON} modes.
 *
 * <p>The counter is incremented <em>after</em> the delegated {@code super}
 * call returns normally so failed attempts followed by a retry do not inflate
 * the count and produce a misleading “Liquibase ran twice” signal.
 *
 * <p>Package-private intentionally: this is an internal testkit seam, not a
 * public API.
 */
class CountingLiquibaseExecutor extends SimpleLiquibaseExecutor {

    private final AtomicInteger executionCount = new AtomicInteger(0);

    /**
     * @return the number of times {@link #createDatabase(DataSource, LiquibaseModel)}
     *         has completed successfully (i.e. without throwing from {@code super}).
     */
    int executionCount() {
        return executionCount.get();
    }

    @Override
    public void createDatabase(DataSource dataSource, LiquibaseModel liquibaseModel) {
        super.createDatabase(dataSource, liquibaseModel);
        executionCount.incrementAndGet();
    }
}
