package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import com.google.inject.Module;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptor;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Value-equal key used to identify a cached runtime in {@link JudoTestExtension}'s
 * {@code BY_CLASS} and {@code SINGLETON} caches. Two {@link ByClassCacheKey}
 * instances are equal iff every field is equal — see the {@code Cache Key
 * Includes Modules and Interceptors} requirement in the {@code guice-testkit}
 * spec.
 *
 * <p>For {@code SINGLETON} mode the {@code testClass} field is {@code null}
 * so two distinct test classes with otherwise identical configuration share
 * a cached runtime.
 *
 * <p>Package-private intentionally: an internal testkit seam.
 */
final class ByClassCacheKey {

    private final String modelName;
    private final String dialect;
    private final JudoTest.ModelSource modelSource;
    private final Class<?> testClass; // null for SINGLETON
    private final List<Class<? extends Module>> modules;
    private final List<Class<? extends OperationCallInterceptor>> interceptors;

    private ByClassCacheKey(
            String modelName,
            String dialect,
            JudoTest.ModelSource modelSource,
            Class<?> testClass,
            List<Class<? extends Module>> modules,
            List<Class<? extends OperationCallInterceptor>> interceptors) {
        this.modelName = modelName;
        this.dialect = dialect;
        this.modelSource = modelSource;
        this.testClass = testClass;
        this.modules = modules;
        this.interceptors = interceptors;
    }

    static ByClassCacheKey forByClass(JudoTest annotation, String resolvedDialect, Class<?> testClass) {
        return new ByClassCacheKey(
                annotation.modelName(),
                resolvedDialect,
                annotation.modelSource(),
                Objects.requireNonNull(testClass, "testClass"),
                Arrays.asList(annotation.modules()),
                Arrays.asList(annotation.interceptors()));
    }

    static ByClassCacheKey forSingleton(JudoTest annotation, String resolvedDialect) {
        return new ByClassCacheKey(
                annotation.modelName(),
                resolvedDialect,
                annotation.modelSource(),
                null,
                Arrays.asList(annotation.modules()),
                Arrays.asList(annotation.interceptors()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ByClassCacheKey)) return false;
        ByClassCacheKey that = (ByClassCacheKey) o;
        return Objects.equals(modelName, that.modelName)
                && Objects.equals(dialect, that.dialect)
                && modelSource == that.modelSource
                && Objects.equals(testClass, that.testClass)
                && Objects.equals(modules, that.modules)
                && Objects.equals(interceptors, that.interceptors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelName, dialect, modelSource, testClass, modules, interceptors);
    }

    @Override
    public String toString() {
        return "ByClassCacheKey{"
                + "modelName='" + modelName + '\''
                + ", dialect='" + dialect + '\''
                + ", modelSource=" + modelSource
                + ", testClass=" + (testClass == null ? "<singleton>" : testClass.getName())
                + ", modules=" + modules
                + ", interceptors=" + interceptors
                + '}';
    }
}
