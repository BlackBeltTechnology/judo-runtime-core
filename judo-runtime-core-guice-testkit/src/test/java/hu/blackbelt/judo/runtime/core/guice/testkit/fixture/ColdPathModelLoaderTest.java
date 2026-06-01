package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Regression guard for the CodeRabbit major finding: the cold path of
 * {@code JudoTestExtension.beforeEach} previously read the class-scoped
 * {@code MODEL_LOADER_KEY} unconditionally, so a method-level
 * {@code @JudoTest} override silently picked up the surrounding class's
 * preloaded model instead of loading its own.
 *
 * <p>The fix gates that read on {@code isClassLevel == true}, exposed via the
 * {@link JudoTestExtension#coldPathModelLoader(boolean, ExtensionContext.Store)}
 * package-private helper so the rule is unit-testable without a real JUnit
 * lifecycle.
 */
@DisplayName("JudoTestExtension.coldPathModelLoader(): class-level reuses, method-level does not")
class ColdPathModelLoaderTest {

    @Test
    void classLevelReadsCachedLoader() {
        ExtensionContext.Store store = mock(ExtensionContext.Store.class);
        JudoModelLoader cachedLoader = mock(JudoModelLoader.class);
        when(store.get(any())).thenReturn(cachedLoader);

        assertSame(cachedLoader, JudoTestExtension.coldPathModelLoader(true, store),
                "class-level @JudoTest MUST reuse the class-scoped preloaded model");
    }

    @Test
    void methodLevelIgnoresCachedLoader() {
        ExtensionContext.Store store = mock(ExtensionContext.Store.class);
        JudoModelLoader cachedLoader = mock(JudoModelLoader.class);
        when(store.get(any())).thenReturn(cachedLoader);

        assertNull(JudoTestExtension.coldPathModelLoader(false, store),
                "method-level @JudoTest MUST NOT silently inherit the class-level model loader");

        // Defensive: the helper must not even touch the store on the method-level path.
        verify(store, never()).get(any());
    }

    @Test
    void classLevelWithEmptyStoreReturnsNull() {
        ExtensionContext.Store store = mock(ExtensionContext.Store.class);
        when(store.get(any())).thenReturn(null);

        assertNull(JudoTestExtension.coldPathModelLoader(true, store),
                "no preloaded model in the class-scoped store -> null (caller falls through to fresh prepare)");
    }
}
