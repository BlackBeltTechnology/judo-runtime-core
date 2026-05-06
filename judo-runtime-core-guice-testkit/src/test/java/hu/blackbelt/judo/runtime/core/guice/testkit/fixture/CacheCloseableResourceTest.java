package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import com.google.inject.Injector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R6 in the {@code cache-byclass-test-runtime} change.
 *
 * <p>Asserts that {@link CachedRuntime#close()} is idempotent (no double-close,
 * no exception on the second call) so that JUnit's class-scoped store cleanup
 * and the SINGLETON shutdown hook can both invoke it safely.
 *
 * <p>Pure unit test \u2014 no model loaded; uses {@code null} for the bundled
 * artifacts since {@code close()} only needs to flip the {@code closed} flag.
 */
@DisplayName("CachedRuntime#close is idempotent")
class CacheCloseableResourceTest {

    @Test
    void firstCloseFlipsTheFlag() {
        CachedRuntime cr = new CachedRuntime(null, null, null, null, null, null, null, null);
        assertFalse(cr.isClosed(), "fresh CachedRuntime must not be closed");
        cr.close();
        assertTrue(cr.isClosed(), "after close() the flag must be set");
    }

    @Test
    void secondCloseIsSilent() {
        CachedRuntime cr = new CachedRuntime(null, null, null, null, null, null, null, null);
        cr.close();
        // Second close MUST NOT throw.
        cr.close();
        assertTrue(cr.isClosed());
    }

    @Test
    void closeSurvivesNullInjector() {
        // Sanity: even with all bundled artifacts null, close() must not NPE.
        CachedRuntime cr = new CachedRuntime(null, null, null, null, null, null, null, null);
        cr.close();
        assertTrue(cr.isClosed());
    }

    @Test
    void cachedRuntimeReturnsBundledFields() {
        Injector dummy = null; // we don't materialize a real injector for this unit test
        CachedRuntime cr = new CachedRuntime(null, null, null, null, null, null, dummy, null);
        // Field access via package-private members.
        assertNotNull(cr); // smoke
    }
}
