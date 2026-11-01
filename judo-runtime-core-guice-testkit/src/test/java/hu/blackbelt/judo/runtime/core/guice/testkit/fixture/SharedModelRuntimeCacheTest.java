package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * JNG-6421: with {@code judo.testkit.shareModel=true}, {@code BY_CLASS} classes reuse one
 * {@link ModelRuntime} (model loader + coercer + QueryFactory) per
 * {@code (modelName, dialect, modelSource)} JVM-wide.
 *
 * <p>Pure unit test — the model loader and the ModelRuntime are produced by test factories,
 * so no real JUDO model is needed. End-to-end behaviour (same QueryFactory, different
 * Injector, isolated database) is verified by the consumer suites.
 */
@DisplayName("Shared model runtime cache: keyed by (modelName, dialect, modelSource)")
class SharedModelRuntimeCacheTest {

    private static final SingletonModelKey KEY =
            new SingletonModelKey("m", "hsqldb", JudoTest.ModelSource.AUTO);

    private final AtomicInteger loads = new AtomicInteger();
    private final AtomicInteger builds = new AtomicInteger();

    @BeforeEach
    @AfterEach
    void clearMaps() {
        JudoTestExtension.closeAllSingletonRuntimes();
        JudoTestExtension.clearSharedModelRuntimesForTesting();
    }

    private ModelRuntime lookup(SingletonModelKey key) throws Exception {
        return JudoTestExtension.sharedModelRuntime(key,
                k -> {
                    loads.incrementAndGet();
                    return mock(JudoModelLoader.class);
                },
                loader -> {
                    builds.incrementAndGet();
                    return new ModelRuntime(loader, null, mock(QueryFactory.class));
                });
    }

    @Test
    void equalKeysShareOneModelRuntime() throws Exception {
        ModelRuntime first = lookup(KEY);
        ModelRuntime second = lookup(new SingletonModelKey("m", "hsqldb", JudoTest.ModelSource.AUTO));

        assertSame(first, second, "equal keys MUST share one ModelRuntime");
        assertSame(first.queryFactory, second.queryFactory);
        assertEquals(1, loads.get(), "the model MUST be loaded once per key");
        assertEquals(1, builds.get(), "the QueryFactory MUST be built once per key");
    }

    @Test
    void distinctModelNamesDoNotShare() throws Exception {
        assertNotSame(lookup(KEY), lookup(new SingletonModelKey("other", "hsqldb", JudoTest.ModelSource.AUTO)));
    }

    @Test
    void distinctDialectsDoNotShare() throws Exception {
        assertNotSame(lookup(KEY), lookup(new SingletonModelKey("m", "postgresql", JudoTest.ModelSource.AUTO)));
    }

    @Test
    void distinctModelSourcesDoNotShare() throws Exception {
        assertNotSame(lookup(KEY), lookup(new SingletonModelKey("m", "hsqldb", JudoTest.ModelSource.CLASSPATH)));
    }

    @Test
    void reusesAnAlreadyLoadedSingletonModelLoader() throws Exception {
        JudoModelLoader preloaded = mock(JudoModelLoader.class);
        JudoTestExtension.singletonModelLoadersForTesting().put(KEY, preloaded);

        ModelRuntime runtime = lookup(KEY);

        assertSame(preloaded, runtime.modelLoader, "a SINGLETON-loaded model MUST be reused, not reloaded");
        assertEquals(0, loads.get());
    }

    @Test
    void storesTheLoadedModelInTheSingletonLoaderMap() throws Exception {
        ModelRuntime runtime = lookup(KEY);

        assertSame(runtime.modelLoader, JudoTestExtension.singletonModelLoadersForTesting().get(KEY));
    }

    /**
     * Surefire runs one JUnit launcher session per test class in a reused fork, and the root store
     * (and with it {@code closeAllSingletonRuntimes}) closes at the end of every session. The shared
     * model runtime holds no resources, only memory, so it MUST survive that close — otherwise every
     * fork rebuilds it after its first class.
     */
    @Test
    void survivesTheEndOfALauncherSession() throws Exception {
        ModelRuntime before = lookup(KEY);

        JudoTestExtension.closeAllSingletonRuntimes();

        assertSame(before, lookup(KEY), "the shared model runtime MUST outlive a launcher session");
        assertEquals(1, builds.get());
    }
}
