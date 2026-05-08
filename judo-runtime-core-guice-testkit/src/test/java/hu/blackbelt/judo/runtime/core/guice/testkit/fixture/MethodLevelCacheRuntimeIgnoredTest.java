package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Verifies that the {@code cacheRuntime} flag is IGNORED when {@code @JudoTest}
 * is applied at the method level: method-level annotations always behave as
 * {@code BY_METHOD}, regardless of any other annotation values.
 *
 * <p>Pure unit test \u2014 directly probes the routing predicate.
 */
@DisplayName("Method-level @JudoTest ignores cacheRuntime (always BY_METHOD)")
class MethodLevelCacheRuntimeIgnoredTest {

    @Test
    @DisplayName("Method-level + cacheRuntime=true is identical to method-level + cacheRuntime=false")
    void methodLevelTrueEqualsFalse() {
        boolean withTrue = JudoTestExtensionRouting.useCache(
                /* isClassLevel */ false, JudoTest.DataSourceMode.BY_CLASS, true);
        boolean withFalse = JudoTestExtensionRouting.useCache(
                /* isClassLevel */ false, JudoTest.DataSourceMode.BY_CLASS, false);

        assertFalse(withTrue, "method-level annotation always bypasses cache");
        assertFalse(withFalse, "method-level annotation always bypasses cache");
    }
}
