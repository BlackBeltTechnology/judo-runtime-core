package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import java.util.Objects;

/**
 * Value-equal key used to identify a cached {@code CloseableDatasourceFixture} in
 * the JVM-wide SINGLETON datasource map held by {@link JudoTestExtension}. Two
 * instances are equal iff every field is equal.
 *
 * <p>Keying is narrower than {@link ByClassCacheKey} because the datasource itself
 * is determined only by {@code (dialect, container)} — the test class, model name,
 * model source, Guice modules, and interceptors do not affect which physical
 * database backs the fixture.
 *
 * <p>Package-private intentionally: an internal testkit seam.
 */
final class SingletonDatasourceKey {

    private final String dialect;
    private final String container;

    SingletonDatasourceKey(String dialect, String container) {
        this.dialect = dialect;
        this.container = container;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SingletonDatasourceKey)) return false;
        SingletonDatasourceKey that = (SingletonDatasourceKey) o;
        return Objects.equals(dialect, that.dialect)
                && Objects.equals(container, that.container);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dialect, container);
    }

    @Override
    public String toString() {
        return "SingletonDatasourceKey{"
                + "dialect='" + dialect + '\''
                + ", container='" + container + '\''
                + '}';
    }
}
