package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Functional verification of {@link CountingLiquibaseExecutor}.
 *
 * <p>The cache change relies on this counting wrapper to prove that Liquibase
 * runs exactly once per cached runtime in {@code BY_CLASS} / {@code SINGLETON}
 * modes. This test verifies the wrapper's contract independently of any real
 * Liquibase machinery: the counter MUST increment on every entry to
 * {@link CountingLiquibaseExecutor#createDatabase}, regardless of whether the
 * delegated {@code super.createDatabase} call succeeds.
 *
 * <p>We do not pass a real {@link javax.sql.DataSource} \u2014 the
 * {@code super.createDatabase} call is expected to fail on null inputs, but the
 * counter increment happens before the super-call and so is observable.
 */
@DisplayName("CountingLiquibaseExecutor: counter increments on every createDatabase call")
class CountingLiquibaseExecutorTest {

    @Test
    void freshExecutorReportsZero() {
        assertEquals(0, new CountingLiquibaseExecutor().executionCount(),
                "a freshly-constructed counting executor must report zero invocations");
    }

    @Test
    void counterIncrementsBeforeDelegating() {
        CountingLiquibaseExecutor ex = new CountingLiquibaseExecutor();

        // super.createDatabase will throw on null inputs \u2014 that's fine, the
        // contract we care about is that the counter is incremented BEFORE
        // the super-call, so the increment is observable.
        assertThrows(Throwable.class, () -> ex.createDatabase(null, null));
        assertEquals(1, ex.executionCount(),
                "after one (failed) createDatabase call the counter must be 1");

        assertThrows(Throwable.class, () -> ex.createDatabase(null, null));
        assertEquals(2, ex.executionCount(),
                "after two (failed) createDatabase calls the counter must be 2");
    }

    @Test
    void counterIsAtomicAcrossThreads() throws InterruptedException {
        CountingLiquibaseExecutor ex = new CountingLiquibaseExecutor();
        int threads = 8;
        int callsPerThread = 25;
        Thread[] workers = new Thread[threads];

        for (int t = 0; t < threads; t++) {
            workers[t] = new Thread(() -> {
                for (int i = 0; i < callsPerThread; i++) {
                    try {
                        ex.createDatabase(null, null);
                    } catch (Throwable ignored) {
                        // expected: super throws on null inputs
                    }
                }
            });
        }
        for (Thread w : workers) w.start();
        for (Thread w : workers) w.join();

        assertEquals(threads * callsPerThread, ex.executionCount(),
                "AtomicInteger guarantees no lost updates under concurrent createDatabase calls");
    }
}
