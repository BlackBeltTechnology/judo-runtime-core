package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R7 in the {@code cache-byclass-test-runtime} change.
 *
 * <p>Guards the design invariant that {@link JudoRuntimeFixture#init(Module, Object)}
 * remains virtual (non-final, non-static) so that user subclasses can override it
 * and have the override invoked on the BY_METHOD code path. The change in this
 * proposal explicitly documents that overrides do NOT fire on the cached
 * BY_CLASS / SINGLETON path \u2014 users who rely on overriding {@code init} must
 * pin their tests to {@code BY_METHOD}.
 *
 * <p>Today, {@link JudoTestExtension} instantiates a plain {@link JudoRuntimeFixture},
 * so the override would only fire if a user provides their own extension or constructs
 * the fixture directly. This test verifies the polymorphic-dispatch contract via a
 * direct call.
 */
@DisplayName("JudoRuntimeFixture#init remains overridable for BY_METHOD subclasses")
class BackwardsCompatBuildModeTest {

    static class CountingFixture extends JudoRuntimeFixture {
        int initInvocations = 0;
        @Override
        public void init(Module module, Object injectModulesTo) {
            initInvocations++;
            // intentionally NOT calling super.init: this is a contract test, not an integration test.
        }
    }

    @Test
    void overriddenInitFires() {
        CountingFixture f = new CountingFixture();
        assertFalse(f.initInvocations > 0);
        f.init(new AbstractModule() {}, null);
        assertTrue(f.initInvocations == 1, "subclass override of init() must be invoked polymorphically");
    }
}
