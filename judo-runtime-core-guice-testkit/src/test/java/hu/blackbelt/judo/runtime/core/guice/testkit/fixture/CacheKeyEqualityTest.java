package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * R5 in the {@code cache-byclass-test-runtime} change.
 *
 * <p>Asserts that {@link ByClassCacheKey} \u2014 the cache key for {@code BY_CLASS}
 * and {@code SINGLETON} runtime caching \u2014 distinguishes configurations that
 * differ only in {@code modules} or {@code interceptors}, so two such test
 * configurations do NOT share a cached runtime.
 *
 * <p>Pure unit test \u2014 no model loaded.
 */
@DisplayName("ByClassCacheKey distinguishes by modules and interceptors")
class CacheKeyEqualityTest {

    static class ModuleA extends AbstractModule {}
    static class ModuleB extends AbstractModule {}

    static class InterceptorA implements OperationCallInterceptor {
        @Override public String getName() { return "a"; }
    }
    static class InterceptorB implements OperationCallInterceptor {
        @Override public String getName() { return "b"; }
    }

    @Test
    void identicalAnnotationsProduceEqualKeys() {
        JudoTest a = annotation("m", "hsqldb", JudoTest.ModelSource.AUTO,
                modules(ModuleA.class), interceptors(InterceptorA.class));
        JudoTest b = annotation("m", "hsqldb", JudoTest.ModelSource.AUTO,
                modules(ModuleA.class), interceptors(InterceptorA.class));

        assertEquals(ByClassCacheKey.forSingleton(a, "hsqldb"),
                     ByClassCacheKey.forSingleton(b, "hsqldb"));
        assertEquals(ByClassCacheKey.forByClass(a, "hsqldb", CacheKeyEqualityTest.class),
                     ByClassCacheKey.forByClass(b, "hsqldb", CacheKeyEqualityTest.class));
    }

    @Test
    void differingModulesProduceDifferentKeys() {
        JudoTest a = annotation("m", "hsqldb", JudoTest.ModelSource.AUTO,
                modules(ModuleA.class), interceptors());
        JudoTest b = annotation("m", "hsqldb", JudoTest.ModelSource.AUTO,
                modules(ModuleB.class), interceptors());

        assertNotEquals(ByClassCacheKey.forSingleton(a, "hsqldb"),
                        ByClassCacheKey.forSingleton(b, "hsqldb"),
                "Different modules MUST yield different cache entries");
    }

    @Test
    void differingInterceptorsProduceDifferentKeys() {
        JudoTest a = annotation("m", "hsqldb", JudoTest.ModelSource.AUTO,
                modules(), interceptors(InterceptorA.class));
        JudoTest b = annotation("m", "hsqldb", JudoTest.ModelSource.AUTO,
                modules(), interceptors(InterceptorB.class));

        assertNotEquals(ByClassCacheKey.forSingleton(a, "hsqldb"),
                        ByClassCacheKey.forSingleton(b, "hsqldb"),
                "Different interceptors MUST yield different cache entries");
    }

    @Test
    void differingTestClassProducesDifferentByClassKeys() {
        JudoTest a = annotation("m", "hsqldb", JudoTest.ModelSource.AUTO, modules(), interceptors());

        assertNotEquals(ByClassCacheKey.forByClass(a, "hsqldb", CacheKeyEqualityTest.class),
                        ByClassCacheKey.forByClass(a, "hsqldb", String.class));
    }

    /* ------------------------------------------------------------------ */

    @SafeVarargs
    private static Class<? extends Module>[] modules(Class<? extends Module>... cs) { return cs; }

    @SafeVarargs
    private static Class<? extends OperationCallInterceptor>[] interceptors(Class<? extends OperationCallInterceptor>... cs) { return cs; }

    /**
     * Builds a minimal {@link JudoTest} annotation proxy carrying only the
     * fields {@link ByClassCacheKey} consumes.
     */
    private static JudoTest annotation(
            String modelName,
            String dialect,
            JudoTest.ModelSource modelSource,
            Class<? extends Module>[] modules,
            Class<? extends OperationCallInterceptor>[] interceptors) {
        return new JudoTest() {
            @Override public Class<? extends Annotation> annotationType() { return JudoTest.class; }
            @Override public String modelName() { return modelName; }
            @Override public String dialect() { return dialect; }
            @Override public String container() { return "none"; }
            @Override public TransactionHandling transaction() { return TransactionHandling.AUTO_ROLLBACK; }
            @Override public boolean truncateTables() { return true; }
            @Override public ModelSource modelSource() { return modelSource; }
            @Override public Class<? extends Module>[] modules() { return modules; }
            @Override public DataSourceMode dataSourceMode() { return DataSourceMode.BY_CLASS; }
            @Override public Class<? extends OperationCallInterceptor>[] interceptors() { return interceptors; }
        };
    }
}
