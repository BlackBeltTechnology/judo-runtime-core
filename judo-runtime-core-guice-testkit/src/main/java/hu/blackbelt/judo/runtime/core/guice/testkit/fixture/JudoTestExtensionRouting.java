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
     * Decides whether {@code beforeEach} should route through the runtime cache (fast path)
     * or the cold path that calls {@code prepare(…) → init(…)} on a fresh
     * {@link JudoRuntimeFixture}.
     *
     * <p>The {@code shareInjector} flag is honoured <b>uniformly</b> for every mode that
     * has a class-scoped datasource. There are no per-mode carve-outs:
     * <ul>
     *   <li>{@code BY_METHOD} — always cold path (nothing to cache).</li>
     *   <li>Method-level annotation (any mode) — always cold path
     *       (caching is a class-scoped concept).</li>
     *   <li>{@code BY_CLASS} or {@code SINGLETON} — cache iff {@code shareInjector = true}.</li>
     * </ul>
     *
     * <p>Re-running Liquibase against a shared {@code SINGLETON} datasource when
     * {@code shareInjector = false} is correct: the {@code DATABASECHANGELOG} table
     * makes the re-application a no-op for already-applied changesets, and
     * {@code DATABASECHANGELOGLOCK} serialises concurrent invocations. The cost is
     * performance (extra round-trip per method), not correctness.
     */
    static boolean useCache(boolean isClassLevel, JudoTest.DataSourceMode mode, boolean shareInjector) {
        if (!isClassLevel) {
            return false;
        }
        if (mode == JudoTest.DataSourceMode.BY_METHOD) {
            return false;
        }
        return shareInjector;
    }
}
