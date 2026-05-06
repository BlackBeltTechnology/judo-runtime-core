package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Functional verification of the JVM-wide SINGLETON cache held by
 * {@link JudoTestExtension#singletonRuntimesForTesting()}.
 *
 * <p>Asserts the contract that two equal {@link ByClassCacheKey}s map to the
 * SAME cached runtime instance, that distinct keys map to DIFFERENT instances,
 * and that {@code closeAllSingletonRuntimes()} closes every entry exactly once
 * and clears the map (the JVM-shutdown cleanup path).
 *
 * <p>Pure unit test \u2014 no model loaded; uses {@link CachedRuntime} instances
 * built with {@code null} bundled artifacts since only identity and lifecycle
 * are under test.
 */
@DisplayName("SINGLETON runtime map: identity, distinctness, and shutdown")
class SingletonRuntimeMapTest {

    @BeforeEach
    @AfterEach
    void clearMap() {
        // Defensive: ensure no leakage from / to other tests touching the static map.
        JudoTestExtension.closeAllSingletonRuntimes();
    }

    @Test
    void putAndGetByEqualKeyReturnsSameInstance() {
        Map<ByClassCacheKey, CachedRuntime> map = JudoTestExtension.singletonRuntimesForTesting();

        ByClassCacheKey k1 = ByClassCacheKey.forSingleton(annotation("m"), "hsqldb");
        ByClassCacheKey k2 = ByClassCacheKey.forSingleton(annotation("m"), "hsqldb");
        assertEquals(k1, k2, "value-equal keys must compare equal");
        assertNotSame(k1, k2, "but be different object instances");

        CachedRuntime cached = newCached();
        map.put(k1, cached);

        assertSame(cached, map.get(k2),
                "lookup by an equal-but-distinct key MUST return the same cached instance");
    }

    @Test
    void distinctKeysMapToDistinctEntries() {
        Map<ByClassCacheKey, CachedRuntime> map = JudoTestExtension.singletonRuntimesForTesting();

        ByClassCacheKey kA = ByClassCacheKey.forSingleton(annotation("modelA"), "hsqldb");
        ByClassCacheKey kB = ByClassCacheKey.forSingleton(annotation("modelB"), "hsqldb");

        CachedRuntime a = newCached();
        CachedRuntime b = newCached();
        map.put(kA, a);
        map.put(kB, b);

        assertSame(a, map.get(kA));
        assertSame(b, map.get(kB));
        assertNotSame(map.get(kA), map.get(kB),
                "distinct configurations MUST NOT share a cached runtime");
    }

    @Test
    void shutdownClosesEveryEntryExactlyOnceAndClearsTheMap() {
        Map<ByClassCacheKey, CachedRuntime> map = JudoTestExtension.singletonRuntimesForTesting();

        CachedRuntime a = newCached();
        CachedRuntime b = newCached();
        map.put(ByClassCacheKey.forSingleton(annotation("a"), "hsqldb"), a);
        map.put(ByClassCacheKey.forSingleton(annotation("b"), "hsqldb"), b);

        assertFalse(a.isClosed());
        assertFalse(b.isClosed());

        JudoTestExtension.closeAllSingletonRuntimes();

        assertTrue(a.isClosed(), "every entry MUST be closed at JVM shutdown");
        assertTrue(b.isClosed());
        assertTrue(map.isEmpty(), "the static map MUST be cleared after shutdown");

        // Idempotency: a second shutdown call must not throw and must remain a no-op.
        JudoTestExtension.closeAllSingletonRuntimes();
        assertTrue(a.isClosed());
        assertTrue(b.isClosed());
    }

    /* ---------------- helpers ---------------- */

    private static CachedRuntime newCached() {
        return new CachedRuntime(null, null, null, null, null, null, null, null);
    }

    private static JudoTest annotation(String modelName) {
        return new JudoTest() {
            @Override public Class<? extends java.lang.annotation.Annotation> annotationType() { return JudoTest.class; }
            @Override public String modelName() { return modelName; }
            @Override public String dialect() { return "hsqldb"; }
            @Override public String container() { return "none"; }
            @Override public TransactionHandling transaction() { return TransactionHandling.AUTO_ROLLBACK; }
            @Override public boolean truncateTables() { return true; }
            @Override public ModelSource modelSource() { return ModelSource.AUTO; }
            @SuppressWarnings("unchecked")
            @Override public Class<? extends com.google.inject.Module>[] modules() {
                return (Class<? extends com.google.inject.Module>[]) new Class<?>[0];
            }
            @Override public DataSourceMode dataSourceMode() { return DataSourceMode.SINGLETON; }
            @SuppressWarnings("unchecked")
            @Override public Class<? extends hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptor>[] interceptors() {
                return (Class<? extends hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptor>[]) new Class<?>[0];
            }
        };
    }
}
