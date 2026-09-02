package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Regression guard for JNG-6374: the JVM-wide SINGLETON datasource cache MUST
 * be keyed by {@code (dialect, container)}.
 *
 * <p>Without keying, the first {@code @JudoTest(SINGLETON)} class wins and
 * every subsequent class with a different {@code dialect} or {@code container}
 * silently receives the first class's {@code JudoDatasourceFixture} — leading
 * to schema mismatch and undefined behaviour.
 *
 * <p>Pure unit test — operates directly on the package-private static map
 * via {@link JudoTestExtension#singletonDatasourcesForTesting()}.
 */
@DisplayName("SINGLETON datasource cache: keyed by (dialect, container)")
class SingletonDatasourceKeyingTest {

    @BeforeEach
    @AfterEach
    void clearMap() {
        JudoTestExtension.closeAllSingletonRuntimes();
    }

    @Test
    void distinctDialectsYieldDistinctEntries() {
        Map<SingletonDatasourceKey, JudoTestExtension.CloseableDatasourceFixture> map =
                JudoTestExtension.singletonDatasourcesForTesting();

        SingletonDatasourceKey kHsql = new SingletonDatasourceKey("hsqldb", "none");
        SingletonDatasourceKey kPg   = new SingletonDatasourceKey("postgresql", "postgresql");

        JudoTestExtension.CloseableDatasourceFixture hsql =
                new JudoTestExtension.CloseableDatasourceFixture(mock(JudoDatasourceFixture.class));
        JudoTestExtension.CloseableDatasourceFixture pg =
                new JudoTestExtension.CloseableDatasourceFixture(mock(JudoDatasourceFixture.class));
        map.put(kHsql, hsql);
        map.put(kPg, pg);

        assertSame(hsql, map.get(kHsql));
        assertSame(pg, map.get(kPg));
        assertNotSame(map.get(kHsql), map.get(kPg),
                "two SINGLETON classes with different dialect MUST NOT share a datasource");
    }

    @Test
    void distinctContainersYieldDistinctEntries() {
        Map<SingletonDatasourceKey, JudoTestExtension.CloseableDatasourceFixture> map =
                JudoTestExtension.singletonDatasourcesForTesting();

        SingletonDatasourceKey kNone = new SingletonDatasourceKey("postgresql", "none");
        SingletonDatasourceKey kPg   = new SingletonDatasourceKey("postgresql", "postgresql");

        JudoTestExtension.CloseableDatasourceFixture none =
                new JudoTestExtension.CloseableDatasourceFixture(mock(JudoDatasourceFixture.class));
        JudoTestExtension.CloseableDatasourceFixture pg =
                new JudoTestExtension.CloseableDatasourceFixture(mock(JudoDatasourceFixture.class));
        map.put(kNone, none);
        map.put(kPg, pg);

        assertNotSame(map.get(kNone), map.get(kPg),
                "two SINGLETON classes with different container MUST NOT share a datasource");
    }

    @Test
    void valueEqualKeysReturnSameInstance() {
        Map<SingletonDatasourceKey, JudoTestExtension.CloseableDatasourceFixture> map =
                JudoTestExtension.singletonDatasourcesForTesting();

        SingletonDatasourceKey k1 = new SingletonDatasourceKey("hsqldb", "none");
        SingletonDatasourceKey k2 = new SingletonDatasourceKey("hsqldb", "none");
        assertEquals(k1, k2);
        assertEquals(k1.hashCode(), k2.hashCode());
        assertNotSame(k1, k2);

        JudoTestExtension.CloseableDatasourceFixture ds =
                new JudoTestExtension.CloseableDatasourceFixture(mock(JudoDatasourceFixture.class));
        map.put(k1, ds);
        assertSame(ds, map.get(k2),
                "lookup by an equal-but-distinct key MUST return the same datasource instance");
    }

    @Test
    void closeAllSingletonRuntimesClosesAndClearsTheMap() {
        Map<SingletonDatasourceKey, JudoTestExtension.CloseableDatasourceFixture> map =
                JudoTestExtension.singletonDatasourcesForTesting();

        JudoDatasourceFixture inner1 = mock(JudoDatasourceFixture.class);
        JudoDatasourceFixture inner2 = mock(JudoDatasourceFixture.class);
        map.put(new SingletonDatasourceKey("hsqldb", "none"),
                new JudoTestExtension.CloseableDatasourceFixture(inner1));
        map.put(new SingletonDatasourceKey("postgresql", "postgresql"),
                new JudoTestExtension.CloseableDatasourceFixture(inner2));

        JudoTestExtension.closeAllSingletonRuntimes();

        assertTrue(map.isEmpty(), "datasource map MUST be cleared at JVM shutdown");
        // Each underlying JudoDatasourceFixture must have been torn down exactly once.
        org.mockito.Mockito.verify(inner1, org.mockito.Mockito.times(1)).teardownDatasource();
        org.mockito.Mockito.verify(inner2, org.mockito.Mockito.times(1)).teardownDatasource();
    }
}
