package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import java.util.Objects;

/**
 * Value-equal key used to identify a cached {@code JudoModelLoader} in the JVM-wide
 * SINGLETON model loader map held by {@link JudoTestExtension}. Two instances are
 * equal iff every field is equal.
 *
 * <p>Keying is narrower than {@link ByClassCacheKey} because the loaded model itself
 * is determined only by {@code (modelName, dialect, modelSource)} — the test class,
 * Guice modules, and interceptors do not affect what model is loaded from disk /
 * classpath.
 *
 * <p>Package-private intentionally: an internal testkit seam.
 */
final class SingletonModelKey {

    private final String modelName;
    private final String dialect;
    private final JudoTest.ModelSource modelSource;

    SingletonModelKey(String modelName, String dialect, JudoTest.ModelSource modelSource) {
        this.modelName = modelName;
        this.dialect = dialect;
        this.modelSource = modelSource;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SingletonModelKey)) return false;
        SingletonModelKey that = (SingletonModelKey) o;
        return Objects.equals(modelName, that.modelName)
                && Objects.equals(dialect, that.dialect)
                && modelSource == that.modelSource;
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelName, dialect, modelSource);
    }

    @Override
    public String toString() {
        return "SingletonModelKey{"
                + "modelName='" + modelName + '\''
                + ", dialect='" + dialect + '\''
                + ", modelSource=" + modelSource
                + '}';
    }
}
