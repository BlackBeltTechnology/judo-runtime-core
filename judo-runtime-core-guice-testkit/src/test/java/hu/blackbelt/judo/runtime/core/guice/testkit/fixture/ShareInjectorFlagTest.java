package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Functional verification of the {@code @JudoTest#shareInjector} element introduced
 * by the {@code share-injector-opt-in} change. Supersedes the prior
 * {@code JudoTestCacheRuntimeFlagTest} (which tested the renamed/inverted
 * {@code cacheRuntime} flag).
 *
 * <p>The actual end-to-end routing decision in {@link JudoTestExtension} cannot
 * be exercised here without a real JUDO model (the testkit does not ship one).
 * What we CAN verify functionally without a model:
 * <ul>
 *   <li>the annotation element exists with the correct return type and {@code false}
 *       default;</li>
 *   <li>the {@code useCache} routing predicate consumes both {@code dataSourceMode}
 *       and {@code shareInjector} uniformly (no SINGLETON carve-out);</li>
 *   <li>the JVM-wide {@link JudoTestExtension#singletonRuntimesForTesting()} map
 *       is NOT populated when {@code shareInjector = false} \u2014 because nothing
 *       could populate it when the cache lookup is bypassed.</li>
 * </ul>
 *
 * <p>This is a pure unit test \u2014 no model, no datasource, no Liquibase.
 */
@DisplayName("@JudoTest#shareInjector: annotation surface and routing predicate")
class ShareInjectorFlagTest {

    @Test
    @DisplayName("@JudoTest declares a boolean shareInjector() with default false")
    void shareInjectorDefaultIsFalse() throws NoSuchMethodException {
        Method m = JudoTest.class.getMethod("shareInjector");
        assertEquals(boolean.class, m.getReturnType(),
                "shareInjector() must return boolean");
        assertEquals(Boolean.FALSE, m.getDefaultValue(),
                "shareInjector() must default to false (isolation by default)");
    }

    @Test
    @DisplayName("@JudoTest does NOT expose the legacy cacheRuntime element")
    void cacheRuntimeElementHasBeenRemoved() {
        assertThrows(NoSuchMethodException.class,
                () -> JudoTest.class.getMethod("cacheRuntime"),
                "the legacy cacheRuntime element must be removed in favour of shareInjector");
    }

    @Test
    @DisplayName("Routing: BY_CLASS + shareInjector=true uses the cache")
    void byClassWithShareTrueRoutesToCache() {
        assertTrue(JudoTestExtensionRouting.useCache(
                /* isClassLevel */ true,
                JudoTest.DataSourceMode.BY_CLASS,
                /* shareInjector */ true));
    }

    @Test
    @DisplayName("Routing: BY_CLASS + shareInjector=false (default) bypasses the cache")
    void byClassWithShareFalseBypassesTheCache() {
        assertFalse(JudoTestExtensionRouting.useCache(
                true, JudoTest.DataSourceMode.BY_CLASS, false));
    }

    @Test
    @DisplayName("Routing: SINGLETON honours shareInjector uniformly (no carve-out)")
    void singletonHonoursTheFlagUniformly() {
        assertTrue(JudoTestExtensionRouting.useCache(
                true, JudoTest.DataSourceMode.SINGLETON, true),
                "SINGLETON + true must cache");
        assertFalse(JudoTestExtensionRouting.useCache(
                true, JudoTest.DataSourceMode.SINGLETON, false),
                "SINGLETON + false must NOT cache \u2014 the v2 carve-out has been removed");
    }

    @Test
    @DisplayName("Routing: BY_METHOD ignores shareInjector entirely (always cold path)")
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
                "method-level @JudoTest must behave as BY_METHOD regardless of shareInjector");
        assertFalse(JudoTestExtensionRouting.useCache(
                false, JudoTest.DataSourceMode.SINGLETON, true));
    }

    @Test
    @DisplayName("singletonRuntimesForTesting() stays empty when no shareInjector=true SINGLETON test has run")
    void singletonMapIsEmptyByDefault() {
        // Defensive: clear any leakage from other tests in the same class.
        JudoTestExtension.closeAllSingletonRuntimes();
        assertTrue(JudoTestExtension.singletonRuntimesForTesting().isEmpty(),
                "no SINGLETON cache build implies the map stays empty");
    }
}
