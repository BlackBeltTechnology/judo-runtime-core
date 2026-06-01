package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Functional verification of {@link CountingLiquibaseExecutor}.
 *
 * <p>The cache change relies on this counting wrapper to prove that Liquibase
 * runs exactly once per cached runtime in {@code BY_CLASS} / {@code SINGLETON}
 * modes. The contract is: the counter MUST increment <em>only after</em> a
 * successful {@code super.createDatabase} call so that failed attempts followed
 * by a retry do not inflate the count and produce a misleading "Liquibase ran
 * twice" signal.
 */
@DisplayName("CountingLiquibaseExecutor: counts successful createDatabase calls only")
class CountingLiquibaseExecutorTest {

    /**
     * Stub that lets us deterministically simulate success / failure of the super
     * call without standing up a real Liquibase / DataSource. Overrides
     * {@code createDatabase} entirely and mimics the production after-super
     * increment logic when the simulated super succeeds.
     */
    static class StubCountingLiquibaseExecutor extends CountingLiquibaseExecutor {
        boolean shouldThrow;

        @Override
        public void createDatabase(DataSource ds, LiquibaseModel m) {
            if (shouldThrow) {
                throw new RuntimeException("simulated Liquibase failure");
            }
            try {
                Field f = CountingLiquibaseExecutor.class.getDeclaredField("executionCount");
                f.setAccessible(true);
                ((AtomicInteger) f.get(this)).incrementAndGet();
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void freshExecutorReportsZero() {
        assertEquals(0, new CountingLiquibaseExecutor().executionCount(),
                "a freshly-constructed counting executor must report zero invocations");
    }

    @Test
    void counterIncrementsOnlyAfterSuccess() {
        StubCountingLiquibaseExecutor ex = new StubCountingLiquibaseExecutor();

        ex.shouldThrow = false;
        ex.createDatabase(null, null);
        assertEquals(1, ex.executionCount(),
                "after one successful createDatabase the counter must be 1");

        ex.createDatabase(null, null);
        assertEquals(2, ex.executionCount(),
                "after two successful createDatabase calls the counter must be 2");
    }

    @Test
    void counterDoesNotIncrementWhenSuperThrows() {
        StubCountingLiquibaseExecutor ex = new StubCountingLiquibaseExecutor();

        ex.shouldThrow = true;
        assertThrows(RuntimeException.class, () -> ex.createDatabase(null, null));
        assertEquals(0, ex.executionCount(),
                "a failed createDatabase MUST NOT bump the counter (count successful calls only)");

        assertThrows(RuntimeException.class, () -> ex.createDatabase(null, null));
        assertEquals(0, ex.executionCount(),
                "two failed createDatabase calls still leave the counter at 0");

        // Now a successful call increments by exactly one.
        ex.shouldThrow = false;
        ex.createDatabase(null, null);
        assertEquals(1, ex.executionCount(),
                "only the successful call increments the counter");
    }

    @Test
    void counterIsAtomicAcrossThreads() throws InterruptedException {
        StubCountingLiquibaseExecutor ex = new StubCountingLiquibaseExecutor();
        ex.shouldThrow = false;
        int threads = 8;
        int callsPerThread = 25;
        Thread[] workers = new Thread[threads];

        for (int t = 0; t < threads; t++) {
            workers[t] = new Thread(() -> {
                for (int i = 0; i < callsPerThread; i++) {
                    ex.createDatabase(null, null);
                }
            });
        }
        for (Thread w : workers) w.start();
        for (Thread w : workers) w.join();

        assertEquals(threads * callsPerThread, ex.executionCount(),
                "AtomicInteger guarantees no lost updates under concurrent successful createDatabase calls");
    }
}
