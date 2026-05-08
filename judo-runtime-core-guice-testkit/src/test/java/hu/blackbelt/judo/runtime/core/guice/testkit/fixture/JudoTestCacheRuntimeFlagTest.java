package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Functional verification of the {@code @JudoTest#cacheRuntime} element added by the
 * {@code judo-test-enable-runtime-cache-flag} change.
 *
 * <p>The actual end-to-end routing decision in {@link JudoTestExtension} cannot be
 * exercised here without a real JUDO model (the testkit does not ship one). What
 * we CAN verify functionally without a model:
 * <ul>
 *   <li>the annotation element exists with the correct return type and default;</li>
 *   <li>the {@code useCache} routing predicate consumes both
 *       {@code dataSourceMode} and {@code cacheRuntime};</li>
 *   <li>the JVM-wide {@link JudoTestExtension#singletonRuntimesForTesting()} map
 *       is NOT populated when {@code cacheRuntime = false} \u2014 because nothing
 *       could populate it when the cache lookup is bypassed.</li>
 * </ul>
 *
 * <p>This is a pure unit test \u2014 no model, no datasource, no Liquibase.
 */
@DisplayName("@JudoTest#cacheRuntime: annotation surface and routing predicate")
class JudoTestCacheRuntimeFlagTest {

    @Test
    @DisplayName("@JudoTest declares a boolean cacheRuntime() with default true")
    void cacheRuntimeDefaultIsTrue() throws NoSuchMethodException {
        Method m = JudoTest.class.getMethod("cacheRuntime");
        assertEquals(boolean.class, m.getReturnType(),
                "cacheRuntime() must return boolean");
        assertEquals(Boolean.TRUE, m.getDefaultValue(),
                "cacheRuntime() must default to true (backwards-compatible)");
    }

    @Test
    @DisplayName("Routing: BY_CLASS + cacheRuntime=true uses the cache")
    void byClassWithCacheTrueRoutesToCache() {
        assertTrue(JudoTestExtensionRouting.useCache(
                /* isClassLevel */ true,
                JudoTest.DataSourceMode.BY_CLASS,
                /* cacheRuntime */ true));
    }

    @Test
    @DisplayName("Routing: BY_CLASS + cacheRuntime=false bypasses the cache")
    void byClassWithCacheFalseBypassesTheCache() {
        assertFalse(JudoTestExtensionRouting.useCache(
                true, JudoTest.DataSourceMode.BY_CLASS, false));
    }

    @Test
    @DisplayName("Routing: SINGLETON always caches — cacheRuntime=false is ignored")
    void singletonAlwaysCachesRegardlessOfFlag() {
        assertTrue(JudoTestExtensionRouting.useCache(
                true, JudoTest.DataSourceMode.SINGLETON, true),
                "SINGLETON + true must cache");
        assertTrue(JudoTestExtensionRouting.useCache(
                true, JudoTest.DataSourceMode.SINGLETON, false),
                "SINGLETON + false must STILL cache — flag is ignored for SINGLETON");
    }

    @Test
    @DisplayName("Routing: BY_METHOD ignores cacheRuntime entirely (always cold path)")
    void byMethodAlwaysBypassesCache() {
        assertFalse(JudoTestExtensionRouting.useCache(
                true, JudoTest.DataSourceMode.BY_METHOD, true));
        assertFalse(JudoTestExtensionRouting.useCache(
                true, JudoTest.DataSourceMode.BY_METHOD, false));
    }

    @Test
    @DisplayName("Routing: method-level annotation always bypasses the cache (isClassLevel=false)")
    void methodLevelAnnotationAlwaysBypassesCache() {
        assertFalse(JudoTestExtensionRouting.useCache(
                false, JudoTest.DataSourceMode.BY_CLASS, true),
                "method-level @JudoTest must behave as BY_METHOD regardless of cacheRuntime");
        assertFalse(JudoTestExtensionRouting.useCache(
                false, JudoTest.DataSourceMode.SINGLETON, true));
    }

    @Test
    @DisplayName("singletonRuntimesForTesting() stays empty when no SINGLETON test has run")
    void singletonMapIsEmptyByDefault() {
        // Defensive: clear any leakage from other tests in the same class.
        JudoTestExtension.closeAllSingletonRuntimes();
        assertTrue(JudoTestExtension.singletonRuntimesForTesting().isEmpty(),
                "no SINGLETON cache build implies the map stays empty");
    }
}
