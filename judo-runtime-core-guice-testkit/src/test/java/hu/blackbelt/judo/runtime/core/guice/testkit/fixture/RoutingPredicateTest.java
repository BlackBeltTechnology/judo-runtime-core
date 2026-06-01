package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive unit tests for {@link JudoTestExtensionRouting} predicates.
 *
 * <p>Complements {@link ShareInjectorFlagTest} (which focuses on the flag
 * semantics and annotation surface) by covering {@code isClassScopedMode()}
 * and every input combination of {@code useCache()}.
 *
 * <p>Post {@code share-injector-opt-in}: the {@code useCache} rule is
 * uniform across modes \u2014 there are no per-mode carve-outs. The predicate
 * collapses to:
 * <pre>
 *   useCache = isClassLevel
 *           && mode != BY_METHOD
 *           && shareInjector
 * </pre>
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
    @DisplayName("useCache() \u2014 SINGLETON honours shareInjector uniformly")
    class SingletonHonoursFlag {

        @Test
        @DisplayName("SINGLETON + true = cached")
        void singletonTrue() {
            assertTrue(JudoTestExtensionRouting.useCache(
                    true, JudoTest.DataSourceMode.SINGLETON, true));
        }

        @Test
        @DisplayName("SINGLETON + false = NOT cached (carve-out removed)")
        void singletonFalse() {
            assertFalse(JudoTestExtensionRouting.useCache(
                    true, JudoTest.DataSourceMode.SINGLETON, false),
                    "SINGLETON + shareInjector=false must NOT cache; the v2 always-cache carve-out is gone");
        }

        @Test
        @DisplayName("SINGLETON at method-level + shareInjector=true = not cached (isClassLevel=false overrides)")
        void singletonMethodLevel() {
            assertFalse(JudoTestExtensionRouting.useCache(
                    false, JudoTest.DataSourceMode.SINGLETON, true));
        }

        @Test
        @DisplayName("SINGLETON at method-level + shareInjector=false = not cached (both reasons agree)")
        void singletonMethodLevelFalseFlag() {
            assertFalse(JudoTestExtensionRouting.useCache(
                    false, JudoTest.DataSourceMode.SINGLETON, false));
        }
    }

    @Nested
    @DisplayName("useCache() \u2014 BY_CLASS respects shareInjector")
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
    @DisplayName("useCache() \u2014 BY_METHOD never caches")
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
        @DisplayName("BY_METHOD at method-level + shareInjector=true = not cached")
        void byMethodMethodLevel() {
            assertFalse(JudoTestExtensionRouting.useCache(
                    false, JudoTest.DataSourceMode.BY_METHOD, true));
        }

        @Test
        @DisplayName("BY_METHOD at method-level + shareInjector=false = not cached (both reasons agree)")
        void byMethodMethodLevelFalseFlag() {
            assertFalse(JudoTestExtensionRouting.useCache(
                    false, JudoTest.DataSourceMode.BY_METHOD, false));
        }
    }

    @Nested
    @DisplayName("useCache() \u2014 uniform truth-table sanity check")
    class UniformTruthTable {

        /**
         * Once {@code isClassLevel=true} and {@code mode != BY_METHOD}, the
         * predicate must collapse exactly to {@code shareInjector}.
         */
        @Test
        @DisplayName("BY_CLASS and SINGLETON produce identical outputs under identical inputs")
        void byClassAndSingletonAgree() {
            for (boolean flag : new boolean[] { true, false }) {
                assertEquals(
                        JudoTestExtensionRouting.useCache(true, JudoTest.DataSourceMode.BY_CLASS, flag),
                        JudoTestExtensionRouting.useCache(true, JudoTest.DataSourceMode.SINGLETON, flag),
                        "BY_CLASS and SINGLETON must route identically for shareInjector=" + flag);
            }
        }
    }
}
