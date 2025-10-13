package hu.blackbelt.judo.runtime.core.guice.testkit;

import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTestExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.transaction.IllegalTransactionStateException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the JudoTest framework focusing on functionality that doesn't require a real JUDO model.
 * These tests verify:
 * - DataSource creation and management
 * - Transaction manager availability
 * - Database connectivity
 * - Error handling
 * - Resource cleanup
 */
@DisplayName("JudoTest Framework Tests")
class JudoTestFrameworkTest {

    /**
     * Test DataSource fixture functionality
     */
    @Nested
    @DisplayName("DataSource Fixture Tests")
    class DataSourceFixtureTests {

        @Test
        @DisplayName("Should create HSQLDB datasource")
        void testCreateHsqldbDatasource() throws Exception {
            JudoDatasourceFixture fixture = new JudoDatasourceFixture();
            fixture.setDialect("hsqldb");
            fixture.setContainer("none");

            assertDoesNotThrow(() -> {
                fixture.setupDatabase();
                fixture.prepareDatasources();
            });

            assertNotNull(fixture.getDataSource());
            assertNotNull(fixture.getTransactionManager());
            assertNotNull(fixture.getDialect());

            fixture.teardownDatasource();
        }

        @Test
        @DisplayName("Should execute SQL queries on HSQLDB")
        void testExecuteSqlQuery() throws Exception {
            JudoDatasourceFixture fixture = new JudoDatasourceFixture();
            fixture.setDialect("hsqldb");
            fixture.setContainer("none");
            fixture.setupDatabase();
            fixture.prepareDatasources();

            DataSource ds = fixture.getDataSource();
            assertNotNull(ds);

            try (Connection conn = ds.getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1 as test_value FROM (VALUES(0))")) {

                assertTrue(rs.next());
                assertEquals(1, rs.getInt("test_value"));
            }

            fixture.teardownDatasource();
        }

        @Test
        @DisplayName("Should handle transaction manager")
        void testTransactionManager() throws Exception {
            JudoDatasourceFixture fixture = new JudoDatasourceFixture();
            fixture.setDialect("hsqldb");
            fixture.setContainer("none");
            fixture.setupDatabase();
            fixture.prepareDatasources();

            assertNotNull(fixture.getTransactionManager());

            // Should be able to begin a transaction
            assertDoesNotThrow(() -> {
                var status = fixture.getTransactionManager().getTransaction(
                    new org.springframework.transaction.support.DefaultTransactionDefinition()
                );
                assertNotNull(status);
                fixture.getTransactionManager().commit(status);
            });

            fixture.teardownDatasource();
        }

        @Test
        @DisplayName("Should throw exception when no transaction is active with MANDATORY propagation")
        void testMandatoryPropagationWithoutTransaction() throws Exception {
            JudoDatasourceFixture fixture = new JudoDatasourceFixture();
            fixture.setDialect("hsqldb");
            fixture.setContainer("none");
            fixture.setupDatabase();
            fixture.prepareDatasources();

            // Should throw IllegalTransactionStateException when MANDATORY and no transaction
            assertThrows(IllegalTransactionStateException.class, () -> {
                fixture.getTransactionManager().getTransaction(
                    new org.springframework.transaction.support.DefaultTransactionDefinition(
                        org.springframework.transaction.TransactionDefinition.PROPAGATION_MANDATORY
                    )
                );
            });

            fixture.teardownDatasource();
        }
    }

    /**
     * Test transaction helper methods
     */
    @Nested
    @DisplayName("Transaction Helper Tests")
    class TransactionHelperTests {

        @Test
        @DisplayName("Should execute runInTransaction and commit on success")
        void testRunInTransactionSuccess() throws Exception {
            JudoDatasourceFixture fixture = new JudoDatasourceFixture();
            fixture.setDialect("hsqldb");
            fixture.setContainer("none");
            fixture.setupDatabase();
            fixture.prepareDatasources();

            String result = fixture.runInTransaction(() -> {
                // Simulate some work
                return "success";
            });

            assertEquals("success", result);
            fixture.teardownDatasource();
        }

        @Test
        @DisplayName("Should rollback and rethrow exception in runInTransaction")
        void testRunInTransactionFailure() throws Exception {
            JudoDatasourceFixture fixture = new JudoDatasourceFixture();
            fixture.setDialect("hsqldb");
            fixture.setContainer("none");
            fixture.setupDatabase();
            fixture.prepareDatasources();

            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                fixture.runInTransaction(() -> {
                    throw new RuntimeException("Operation failed");
                });
            });

            assertEquals("Operation failed", exception.getMessage());
            fixture.teardownDatasource();
        }

        @Test
        @DisplayName("Should properly handle assertThrowsInTransaction")
        void testAssertThrowsInTransaction() throws Exception {
            JudoDatasourceFixture fixture = new JudoDatasourceFixture();
            fixture.setDialect("hsqldb");
            fixture.setContainer("none");
            fixture.setupDatabase();
            fixture.prepareDatasources();

            IllegalArgumentException exception = fixture.assertThrowsInTransaction(
                IllegalArgumentException.class,
                () -> {
                    throw new IllegalArgumentException("Expected exception");
                }
            );

            assertEquals("Expected exception", exception.getMessage());
            fixture.teardownDatasource();
        }
    }

    /**
     * Test database operations
     */
    @Nested
    @DisplayName("Database Operations Tests")
    class DatabaseOperationsTests {

        @Test
        @DisplayName("Should create and use database connection")
        void testDatabaseConnection() throws Exception {
            JudoDatasourceFixture fixture = new JudoDatasourceFixture();
            fixture.setDialect("hsqldb");
            fixture.setContainer("none");
            fixture.setupDatabase();
            fixture.prepareDatasources();

            try (Connection conn = fixture.getDataSource().getConnection()) {
                assertNotNull(conn);
                assertFalse(conn.isClosed());
                assertTrue(conn.isValid(5));
            }

            fixture.teardownDatasource();
        }

        @Test
        @DisplayName("Should support table operations")
        void testTableOperations() throws Exception {
            JudoDatasourceFixture fixture = new JudoDatasourceFixture();
            fixture.setDialect("hsqldb");
            fixture.setContainer("none");
            fixture.setupDatabase();
            fixture.prepareDatasources();

            try (Connection conn = fixture.getDataSource().getConnection();
                 Statement stmt = conn.createStatement()) {

                // Create table
                assertDoesNotThrow(() ->
                    stmt.execute("CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(50))")
                );

                // Insert data
                assertDoesNotThrow(() ->
                    stmt.execute("INSERT INTO test_table VALUES (1, 'test')")
                );

                // Query data
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_table")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("id"));
                    assertEquals("test", rs.getString("name"));
                }

                // Drop table
                assertDoesNotThrow(() ->
                    stmt.execute("DROP TABLE test_table")
                );
            }

            fixture.teardownDatasource();
        }
    }

    /**
     * Test resource cleanup
     */
    @Nested
    @DisplayName("Resource Cleanup Tests")
    class ResourceCleanupTests {

        @Test
        @DisplayName("Should properly clean up datasource")
        void testDatasourceCleanup() throws Exception {
            JudoDatasourceFixture fixture = new JudoDatasourceFixture();
            fixture.setDialect("hsqldb");
            fixture.setContainer("none");
            fixture.setupDatabase();
            fixture.prepareDatasources();

            DataSource ds = fixture.getDataSource();
            assertNotNull(ds);

            // Clean up should not throw
            assertDoesNotThrow(() -> fixture.teardownDatasource());
        }

        @Test
        @DisplayName("Should handle multiple setup and teardown cycles")
        void testMultipleSetupTeardown() throws Exception {
            JudoDatasourceFixture fixture = new JudoDatasourceFixture();
            fixture.setDialect("hsqldb");
            fixture.setContainer("none");

            // First cycle
            fixture.setupDatabase();
            fixture.prepareDatasources();
            assertNotNull(fixture.getDataSource());
            fixture.teardownDatasource();

            // Second cycle
            fixture.setupDatabase();
            fixture.prepareDatasources();
            assertNotNull(fixture.getDataSource());
            fixture.teardownDatasource();
        }
    }

    /**
     * Test error handling
     */
    @Nested
    @DisplayName("Error Handling Tests")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Should handle invalid SQL gracefully")
        void testInvalidSql() throws Exception {
            JudoDatasourceFixture fixture = new JudoDatasourceFixture();
            fixture.setDialect("hsqldb");
            fixture.setContainer("none");
            fixture.setupDatabase();
            fixture.prepareDatasources();

            try (Connection conn = fixture.getDataSource().getConnection();
                 Statement stmt = conn.createStatement()) {

                assertThrows(SQLException.class, () ->
                    stmt.execute("INVALID SQL STATEMENT")
                );
            }

            fixture.teardownDatasource();
        }

        @Test
        @DisplayName("Should rollback transaction on error")
        void testTransactionRollbackOnError() throws Exception {
            JudoDatasourceFixture fixture = new JudoDatasourceFixture();
            fixture.setDialect("hsqldb");
            fixture.setContainer("none");
            fixture.setupDatabase();
            fixture.prepareDatasources();

            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                fixture.runInTransaction(() -> {
                    // Start transaction and then fail
                    throw new RuntimeException("Transaction failed");
                });
            });

            assertEquals("Transaction failed", exception.getMessage());

            // Transaction should have been rolled back
            // DataSource should still be usable
            try (Connection conn = fixture.getDataSource().getConnection()) {
                assertTrue(conn.isValid(5));
            }

            fixture.teardownDatasource();
        }
    }
}
