package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive unit tests for {@link JudoTestExtensionRouting} predicates.
 *
 * <p>Complements {@link JudoTestCacheRuntimeFlagTest} which focuses on the
 * {@code cacheRuntime} flag. This class covers {@code isClassScopedMode()}
 * and edge cases of {@code useCache()}.
 */
@DisplayName("JudoTestExtensionRouting predicates")
class RoutingPredicateTest {

    @Nested
    @DisplayName("isClassScopedMode()")
    class IsClassScopedMode {

        @Test
        @DisplayName("BY_CLASS is class-scoped")
        void byClassIsClassScoped() {
            assertTrue(JudoTestExtensionRouting.isClassScopedMode(
                    JudoTest.DataSourceMode.BY_CLASS));
        }

        @Test
        @DisplayName("SINGLETON is class-scoped")
        void singletonIsClassScoped() {
            assertTrue(JudoTestExtensionRouting.isClassScopedMode(
                    JudoTest.DataSourceMode.SINGLETON));
        }

        @Test
        @DisplayName("BY_METHOD is NOT class-scoped")
        void byMethodIsNotClassScoped() {
            assertFalse(JudoTestExtensionRouting.isClassScopedMode(
                    JudoTest.DataSourceMode.BY_METHOD));
        }
    }

    @Nested
    @DisplayName("useCache() — SINGLETON always caches")
    class SingletonAlwaysCaches {

        @Test
        @DisplayName("SINGLETON + true = cached")
        void singletonTrue() {
            assertTrue(JudoTestExtensionRouting.useCache(
                    true, JudoTest.DataSourceMode.SINGLETON, true));
        }

        @Test
        @DisplayName("SINGLETON + false = still cached (flag ignored)")
        void singletonFalse() {
            assertTrue(JudoTestExtensionRouting.useCache(
                    true, JudoTest.DataSourceMode.SINGLETON, false));
        }

        @Test
        @DisplayName("SINGLETON at method-level = not cached (isClassLevel=false overrides)")
        void singletonMethodLevel() {
            assertFalse(JudoTestExtensionRouting.useCache(
                    false, JudoTest.DataSourceMode.SINGLETON, true));
        }
    }

    @Nested
    @DisplayName("useCache() — BY_CLASS respects cacheRuntime")
    class ByClassRespectsFlag {

        @Test
        @DisplayName("BY_CLASS + true = cached")
        void byClassTrue() {
            assertTrue(JudoTestExtensionRouting.useCache(
                    true, JudoTest.DataSourceMode.BY_CLASS, true));
        }

        @Test
        @DisplayName("BY_CLASS + false = not cached")
        void byClassFalse() {
            assertFalse(JudoTestExtensionRouting.useCache(
                    true, JudoTest.DataSourceMode.BY_CLASS, false));
        }

        @Test
        @DisplayName("BY_CLASS at method-level = not cached regardless of flag")
        void byClassMethodLevel() {
            assertFalse(JudoTestExtensionRouting.useCache(
                    false, JudoTest.DataSourceMode.BY_CLASS, true));
            assertFalse(JudoTestExtensionRouting.useCache(
                    false, JudoTest.DataSourceMode.BY_CLASS, false));
        }
    }

    @Nested
    @DisplayName("useCache() — BY_METHOD never caches")
    class ByMethodNeverCaches {

        @Test
        @DisplayName("BY_METHOD + true = not cached")
        void byMethodTrue() {
            assertFalse(JudoTestExtensionRouting.useCache(
                    true, JudoTest.DataSourceMode.BY_METHOD, true));
        }

        @Test
        @DisplayName("BY_METHOD + false = not cached")
        void byMethodFalse() {
            assertFalse(JudoTestExtensionRouting.useCache(
                    true, JudoTest.DataSourceMode.BY_METHOD, false));
        }

        @Test
        @DisplayName("BY_METHOD at method-level = not cached")
        void byMethodMethodLevel() {
            assertFalse(JudoTestExtensionRouting.useCache(
                    false, JudoTest.DataSourceMode.BY_METHOD, true));
        }
    }
}
