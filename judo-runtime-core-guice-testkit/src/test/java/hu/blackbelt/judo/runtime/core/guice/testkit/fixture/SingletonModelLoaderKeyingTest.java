package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import hu.blackbelt.judo.runtime.core.guice.JudoModelLoader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Regression guard for JNG-6374: the JVM-wide SINGLETON model loader cache
 * MUST be keyed by {@code (modelName, dialect, modelSource)}.
 *
 * <p>Without keying, the first {@code @JudoTest(SINGLETON)} class wins and
 * every subsequent class with a different {@code modelName}, {@code dialect},
 * or {@code modelSource} silently receives the first class's {@link JudoModelLoader}.
 *
 * <p>Pure unit test — operates directly on the package-private static map
 * via {@link JudoTestExtension#singletonModelLoadersForTesting()}.
 */
@DisplayName("SINGLETON model loader cache: keyed by (modelName, dialect, modelSource)")
class SingletonModelLoaderKeyingTest {

    @BeforeEach
    @AfterEach
    void clearMap() {
        JudoTestExtension.closeAllSingletonRuntimes();
    }

    @Test
    void distinctModelNamesYieldDistinctEntries() {
        Map<SingletonModelKey, JudoModelLoader> map = JudoTestExtension.singletonModelLoadersForTesting();

        SingletonModelKey kA = new SingletonModelKey("modelA", "hsqldb", JudoTest.ModelSource.AUTO);
        SingletonModelKey kB = new SingletonModelKey("modelB", "hsqldb", JudoTest.ModelSource.AUTO);

        JudoModelLoader a = mock(JudoModelLoader.class);
        JudoModelLoader b = mock(JudoModelLoader.class);
        map.put(kA, a);
        map.put(kB, b);

        assertSame(a, map.get(kA));
        assertSame(b, map.get(kB));
        assertNotSame(map.get(kA), map.get(kB),
                "two SINGLETON classes with different modelName MUST NOT share a JudoModelLoader");
    }

    @Test
    void distinctDialectsYieldDistinctEntries() {
        Map<SingletonModelKey, JudoModelLoader> map = JudoTestExtension.singletonModelLoadersForTesting();

        SingletonModelKey kHsql = new SingletonModelKey("m", "hsqldb", JudoTest.ModelSource.AUTO);
        SingletonModelKey kPg   = new SingletonModelKey("m", "postgresql", JudoTest.ModelSource.AUTO);

        JudoModelLoader hsql = mock(JudoModelLoader.class);
        JudoModelLoader pg   = mock(JudoModelLoader.class);
        map.put(kHsql, hsql);
        map.put(kPg, pg);

        assertNotSame(map.get(kHsql), map.get(kPg),
                "two SINGLETON classes with different dialect MUST NOT share a JudoModelLoader");
    }

    @Test
    void distinctModelSourcesYieldDistinctEntries() {
        Map<SingletonModelKey, JudoModelLoader> map = JudoTestExtension.singletonModelLoadersForTesting();

        SingletonModelKey kAuto = new SingletonModelKey("m", "hsqldb", JudoTest.ModelSource.AUTO);
        SingletonModelKey kCp   = new SingletonModelKey("m", "hsqldb", JudoTest.ModelSource.CLASSPATH);

        JudoModelLoader auto = mock(JudoModelLoader.class);
        JudoModelLoader cp   = mock(JudoModelLoader.class);
        map.put(kAuto, auto);
        map.put(kCp, cp);

        assertNotSame(map.get(kAuto), map.get(kCp),
                "two SINGLETON classes with different modelSource MUST NOT share a JudoModelLoader");
    }

    @Test
    void valueEqualKeysReturnSameInstance() {
        Map<SingletonModelKey, JudoModelLoader> map = JudoTestExtension.singletonModelLoadersForTesting();

        SingletonModelKey k1 = new SingletonModelKey("m", "hsqldb", JudoTest.ModelSource.AUTO);
        SingletonModelKey k2 = new SingletonModelKey("m", "hsqldb", JudoTest.ModelSource.AUTO);
        assertEquals(k1, k2, "value-equal keys must compare equal");
        assertEquals(k1.hashCode(), k2.hashCode());
        assertNotSame(k1, k2, "but be different object instances");

        JudoModelLoader loader = mock(JudoModelLoader.class);
        map.put(k1, loader);
        assertSame(loader, map.get(k2),
                "lookup by an equal-but-distinct key MUST return the same loader instance");
    }

    @Test
    void closeAllSingletonRuntimesClearsTheMap() {
        Map<SingletonModelKey, JudoModelLoader> map = JudoTestExtension.singletonModelLoadersForTesting();
        map.put(new SingletonModelKey("m", "hsqldb", JudoTest.ModelSource.AUTO), mock(JudoModelLoader.class));

        JudoTestExtension.closeAllSingletonRuntimes();

        assertTrue(map.isEmpty(), "model loader map MUST be cleared at JVM shutdown");
    }
}
