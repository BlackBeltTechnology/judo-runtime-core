package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Asserts the headline contract of the {@code share-injector-opt-in} change:
 * <b>a class-level {@code @JudoTest} with no other elements MUST produce a
 * fresh runtime per method.</b>
 *
 * <p>Specifically, the default value of {@link JudoTest#shareInjector()} is
 * {@code false}, and the routing predicate {@link JudoTestExtensionRouting#useCache}
 * therefore returns {@code false} for every class-scoped mode when invoked with
 * the annotation default. The first method of any default-annotated class lands
 * on the cold path (build a fresh Injector, QueryFactory, Liquibase executor,
 * transaction manager) regardless of {@link JudoTest.DataSourceMode}.
 *
 * <p>This is a pure unit test \u2014 it probes the annotation default and the
 * routing predicate. End-to-end runtime identity is exercised by
 * {@code PrepareWithCachedRuntimeTest} and the slow-tagged
 * {@code RackinspectModelClassCachePerformanceTest}.
 */
@DisplayName("Default annotation yields a fresh runtime per method")
class DefaultIsFreshRuntimePerMethodTest {

    @Test
    @DisplayName("BY_CLASS at default (shareInjector=false) bypasses the runtime cache")
    void byClassDefaultBypassesCache() {
        boolean defaultShareInjector = defaultShareInjector();
        assertFalse(defaultShareInjector,
                "the @JudoTest#shareInjector() default must be false");

        assertFalse(
                JudoTestExtensionRouting.useCache(
                        /* isClassLevel */ true,
                        JudoTest.DataSourceMode.BY_CLASS,
                        defaultShareInjector),
                "BY_CLASS + default annotation MUST take the cold path so two methods get distinct Injectors");
    }

    @Test
    @DisplayName("SINGLETON at default (shareInjector=false) also bypasses the runtime cache")
    void singletonDefaultBypassesCache() {
        assertFalse(
                JudoTestExtensionRouting.useCache(
                        /* isClassLevel */ true,
                        JudoTest.DataSourceMode.SINGLETON,
                        defaultShareInjector()),
                "SINGLETON + default annotation MUST take the cold path " +
                        "(JVM-wide datasource and model loader remain shared, runtime is fresh per method)");
    }

    @Test
    @DisplayName("BY_METHOD at default obviously bypasses the cache")
    void byMethodDefaultBypassesCache() {
        assertFalse(
                JudoTestExtensionRouting.useCache(
                        true, JudoTest.DataSourceMode.BY_METHOD, defaultShareInjector()));
    }

    /**
     * Reflects the default value of {@code @JudoTest#shareInjector()} so this
     * test does not silently pass if someone flips the default back to {@code true}.
     */
    private static boolean defaultShareInjector() {
        try {
            return (Boolean) JudoTest.class.getMethod("shareInjector").getDefaultValue();
        } catch (NoSuchMethodException e) {
            throw new AssertionError("@JudoTest must expose a shareInjector() element", e);
        }
    }
}
