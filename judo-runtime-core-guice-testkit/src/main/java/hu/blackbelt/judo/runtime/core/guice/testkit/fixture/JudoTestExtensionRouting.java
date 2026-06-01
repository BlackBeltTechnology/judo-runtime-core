package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

/**
 * Pure routing predicates used by {@link JudoTestExtension} to decide between the
 * cached fast path and the cold path. Extracted as a separate class so the
 * decision can be unit-tested independently of any JUDO model, datasource, or
 * Guice injector.
 *
 * <p>Package-private intentionally: an internal testkit seam.
 */
final class JudoTestExtensionRouting {

    private JudoTestExtensionRouting() {} // no instances

    /**
     * @return whether the given {@link JudoTest.DataSourceMode} implies a class-scoped
     *         lifecycle for the underlying datasource and model loader. Used by both
     *         {@code beforeAll} (to decide whether to initialise at class level) and
     *         {@code beforeEach} (to decide whether to consult the runtime cache).
     */
    static boolean isClassScopedMode(JudoTest.DataSourceMode mode) {
        return mode == JudoTest.DataSourceMode.BY_CLASS
                || mode == JudoTest.DataSourceMode.SINGLETON;
    }

    /**
     * Decides whether {@code beforeEach} should route through the runtime cache or
     * the cold path.
     *
     * <p>The {@code cacheRuntime} flag is <b>only</b> honoured for
     * {@link JudoTest.DataSourceMode#BY_CLASS BY_CLASS}. For all other modes the
     * flag is ignored:
     * <ul>
     *   <li>{@code BY_METHOD} — always cold path (nothing to cache).</li>
     *   <li>{@code SINGLETON} — always cached (disabling the cache on a JVM-wide
     *       DataSource would re-run Liquibase per method against a shared database,
     *       risking schema corruption under parallel execution).</li>
     * </ul>
     */
    static boolean useCache(boolean isClassLevel, JudoTest.DataSourceMode mode, boolean cacheRuntime) {
        if (!isClassLevel) {
            return false;
        }
        if (mode == JudoTest.DataSourceMode.SINGLETON) {
            return true; // SINGLETON always caches — cacheRuntime ignored
        }
        return mode == JudoTest.DataSourceMode.BY_CLASS && cacheRuntime;
    }
}
