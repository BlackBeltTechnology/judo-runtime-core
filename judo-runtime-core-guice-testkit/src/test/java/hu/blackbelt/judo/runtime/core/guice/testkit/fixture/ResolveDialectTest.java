package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.hsqldb.HsqldbDialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.postgresql.PostgresqlDialect;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link JudoRuntimeFixture#resolveDialect(String)}.
 */
@DisplayName("JudoRuntimeFixture.resolveDialect()")
class ResolveDialectTest {

    @Test
    @DisplayName("'hsqldb' returns HsqldbDialect")
    void hsqldbReturnsCorrectDialect() {
        Dialect dialect = JudoRuntimeFixture.resolveDialect("hsqldb");
        assertInstanceOf(HsqldbDialect.class, dialect);
    }

    @Test
    @DisplayName("'postgresql' returns PostgresqlDialect")
    void postgresqlReturnsCorrectDialect() {
        Dialect dialect = JudoRuntimeFixture.resolveDialect("postgresql");
        assertInstanceOf(PostgresqlDialect.class, dialect);
    }

    @Test
    @DisplayName("Unknown dialect throws IllegalArgumentException")
    void unknownDialectThrows() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> JudoRuntimeFixture.resolveDialect("oracle"));
        assertTrue(ex.getMessage().contains("oracle"),
                "Exception message should contain the unsupported dialect name");
    }

    @Test
    @DisplayName("Null dialect throws IllegalArgumentException")
    void nullDialectThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> JudoRuntimeFixture.resolveDialect(null));
    }

    @Test
    @DisplayName("Each call returns a fresh instance (no shared mutable state)")
    void eachCallReturnsFreshInstance() {
        Dialect first = JudoRuntimeFixture.resolveDialect("hsqldb");
        Dialect second = JudoRuntimeFixture.resolveDialect("hsqldb");
        assertNotSame(first, second,
                "resolveDialect should return a new instance each time");
    }
}
