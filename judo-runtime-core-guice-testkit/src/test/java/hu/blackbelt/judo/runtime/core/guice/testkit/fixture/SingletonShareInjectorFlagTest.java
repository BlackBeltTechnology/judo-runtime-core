package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive routing-level coverage of the new
 * {@code SINGLETON + shareInjector} combinations \u2014 the most controversial
 * delta of the {@code share-injector-opt-in} change, because it reverses
 * the v2 {@code judo-test-enable-runtime-cache-flag} carve-out that said
 * SINGLETON must always cache.
 *
 * <p>The change re-evaluated the Liquibase concern: re-running Liquibase
 * against an already-migrated shared database is correct via
 * {@code DATABASECHANGELOG} (idempotence) and concurrency-safe via
 * {@code DATABASECHANGELOGLOCK} (serialised re-entry). The cost is
 * performance, not correctness \u2014 so users are now allowed to choose.
 */
@DisplayName("SINGLETON honours shareInjector uniformly with BY_CLASS")
class SingletonShareInjectorFlagTest {

    @Nested
    @DisplayName("SINGLETON + shareInjector=true \u2014 cached")
    class CachedConfiguration {

        @Test
        @DisplayName("Class-level SINGLETON + true routes through the cache")
        void cachedClassLevel() {
            assertTrue(JudoTestExtensionRouting.useCache(
                    /* isClassLevel */ true,
                    JudoTest.DataSourceMode.SINGLETON,
                    /* shareInjector */ true));
        }

        @Test
        @DisplayName("Method-level SINGLETON + true does NOT cache (class-level guard wins)")
        void methodLevelSingletonStillBypasses() {
            assertFalse(JudoTestExtensionRouting.useCache(
                    false, JudoTest.DataSourceMode.SINGLETON, true));
        }
    }

    @Nested
    @DisplayName("SINGLETON + shareInjector=false (default) \u2014 NOT cached")
    class DefaultConfiguration {

        @Test
        @DisplayName("Class-level SINGLETON + false takes the cold path")
        void coldPathClassLevel() {
            assertFalse(JudoTestExtensionRouting.useCache(
                    /* isClassLevel */ true,
                    JudoTest.DataSourceMode.SINGLETON,
                    /* shareInjector */ false),
                    "SINGLETON without explicit sharing MUST NOT consult the runtime cache");
        }

        @Test
        @DisplayName("Method-level SINGLETON + false takes the cold path (both reasons agree)")
        void coldPathMethodLevel() {
            assertFalse(JudoTestExtensionRouting.useCache(
                    false, JudoTest.DataSourceMode.SINGLETON, false));
        }
    }

    @Nested
    @DisplayName("Identity invariant: BY_CLASS and SINGLETON routing agree under matching inputs")
    class UniformWithByClass {

        @Test
        @DisplayName("Truth-table parity holds for both flag values")
        void truthTableParity() {
            for (boolean flag : new boolean[] { true, false }) {
                assertEquals(
                        JudoTestExtensionRouting.useCache(true, JudoTest.DataSourceMode.BY_CLASS, flag),
                        JudoTestExtensionRouting.useCache(true, JudoTest.DataSourceMode.SINGLETON, flag),
                        "shareInjector=" + flag + " must produce identical routing for BY_CLASS and SINGLETON");
            }
        }
    }
}
