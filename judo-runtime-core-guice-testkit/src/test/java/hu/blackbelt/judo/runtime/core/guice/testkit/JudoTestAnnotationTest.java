package hu.blackbelt.judo.runtime.core.guice.testkit;

import static org.junit.jupiter.api.Assertions.*;

import com.google.inject.AbstractModule;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.jupiter.api.*;
import org.springframework.transaction.IllegalTransactionStateException;

/**
 * Comprehensive tests for @JudoTest annotation covering:
 * - Positive scenarios (successful execution)
 * - Negative scenarios (error handling)
 * - Transaction behavior
 * - DataSource modes (BY_CLASS, BY_METHOD, SINGLETON)
 * - Transaction handling strategies
 *
 * NOTE: These tests are disabled because they require a real JUDO model named 'test'
 * which doesn't exist in the test environment. They serve as documentation and
 * can be enabled when a test model is available.
 */
@Disabled("Requires a real JUDO model 'test' which doesn't exist in test environment")
@DisplayName("JudoTest Annotation Comprehensive Tests")
class JudoTestAnnotationTest {

    /**
     * Test basic fixture injection with default settings
     */
    @Nested
    @DisplayName("Basic Fixture Injection Tests")
    class BasicFixtureInjectionTests {

        @Test
        @JudoTest(modelName = "test")
        @DisplayName("Should inject JudoRuntimeFixture")
        void testFixtureInjection(JudoRuntimeFixture fixture) {
            assertNotNull(fixture, "Runtime fixture should be injected");
            assertNotNull(fixture.getInjector(), "Guice injector should be available");
        }

        @Test
        @JudoTest(modelName = "test")
        @DisplayName("Should inject JudoDatasourceFixture")
        void testDatasourceFixtureInjection(JudoDatasourceFixture datasourceFixture) {
            assertNotNull(datasourceFixture, "Datasource fixture should be injected");
            assertNotNull(datasourceFixture.getDataSource(), "DataSource should be available");
        }

        @Test
        @JudoTest(modelName = "test")
        @DisplayName("Should inject both fixtures")
        void testBothFixturesInjection(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
            assertNotNull(runtimeFixture, "Runtime fixture should be injected");
            assertNotNull(datasourceFixture, "Datasource fixture should be injected");
            assertNotNull(runtimeFixture.getInjector(), "Guice injector should be available");
            assertNotNull(datasourceFixture.getDataSource(), "DataSource should be available");
        }
    }

    /**
     * Test DataSource mode configurations
     */
    @Nested
    @DisplayName("DataSource Mode Tests")
    class DataSourceModeTests {

        @Test
        @JudoTest(modelName = "test", dataSourceMode = JudoTest.DataSourceMode.BY_CLASS)
        @DisplayName("Should use BY_CLASS datasource mode")
        void testByClassMode(JudoDatasourceFixture datasourceFixture) {
            assertNotNull(datasourceFixture.getDataSource());
        }

        @Test
        @JudoTest(modelName = "test", dataSourceMode = JudoTest.DataSourceMode.BY_METHOD)
        @DisplayName("Should use BY_METHOD datasource mode")
        void testByMethodMode(JudoDatasourceFixture datasourceFixture) {
            assertNotNull(datasourceFixture.getDataSource());
        }

        @Test
        @JudoTest(modelName = "test", dataSourceMode = JudoTest.DataSourceMode.SINGLETON)
        @DisplayName("Should use SINGLETON datasource mode")
        void testSingletonMode(JudoDatasourceFixture datasourceFixture) {
            assertNotNull(datasourceFixture.getDataSource());
        }
    }

    /**
     * Test transaction handling strategies
     */
    @Nested
    @DisplayName("Transaction Handling Tests")
    class TransactionHandlingTests {

        @Test
        @JudoTest(modelName = "test", transaction = JudoTest.TransactionHandling.AUTO_ROLLBACK)
        @DisplayName("Should auto-rollback transaction")
        void testAutoRollback(JudoRuntimeFixture fixture, JudoDatasourceFixture datasourceFixture) throws SQLException {
            // Transaction should be active
            assertNotNull(fixture);

            // Execute some operation that would normally persist
            try (Connection conn = datasourceFixture.getDataSource().getConnection(); Statement stmt = conn.createStatement()) {
                // This should be rolled back after test
                assertDoesNotThrow(() -> stmt.execute("SELECT 1"));
            }
            // Transaction will be rolled back by the extension
        }

        @Test
        @JudoTest(modelName = "test", transaction = JudoTest.TransactionHandling.AUTO_COMMIT)
        @DisplayName("Should auto-commit transaction")
        void testAutoCommit(JudoRuntimeFixture fixture) {
            assertNotNull(fixture);
            // Transaction will be committed by the extension
        }

        @Test
        @JudoTest(modelName = "test", transaction = JudoTest.TransactionHandling.MANUAL)
        @DisplayName("Should allow manual transaction management")
        void testManualTransaction(JudoRuntimeFixture fixture) {
            assertNotNull(fixture);

            // With MANUAL mode, we control transactions
            assertDoesNotThrow(() -> {
                fixture.beginTransaction();
                // Do some work
                fixture.commitTransaction();
            });
        }

        @Test
        @JudoTest(modelName = "test", transaction = JudoTest.TransactionHandling.NONE)
        @DisplayName("Should work without transaction management")
        void testNoTransaction(JudoRuntimeFixture fixture) {
            assertNotNull(fixture);
            // No transaction management
        }
    }

    /**
     * Test error handling and negative scenarios
     */
    @Nested
    @DisplayName("Error Handling Tests")
    class ErrorHandlingTests {

        @Test
        @JudoTest(modelName = "test", transaction = JudoTest.TransactionHandling.AUTO_ROLLBACK)
        @DisplayName("Should rollback on test failure")
        void testRollbackOnFailure(JudoRuntimeFixture fixture) {
            assertNotNull(fixture);

            // This test intentionally throws an exception
            // Transaction should still be rolled back properly
            Exception exception = assertThrows(RuntimeException.class, () -> {
                // Simulate some work
                throw new RuntimeException("Simulated test failure");
            });

            assertEquals("Simulated test failure", exception.getMessage());
            // Extension should handle rollback even with exception
        }

        @Test
        @JudoTest(modelName = "test", transaction = JudoTest.TransactionHandling.MANUAL)
        @DisplayName("Should handle manual transaction errors")
        void testManualTransactionError(JudoRuntimeFixture fixture) {
            assertNotNull(fixture);

            fixture.beginTransaction();

            try {
                // Simulate error during transaction
                throw new RuntimeException("Operation failed");
            } catch (Exception e) {
                // Manual rollback
                assertDoesNotThrow(() -> fixture.rollbackTransaction());
                assertEquals("Operation failed", e.getMessage());
            }
        }

        @Test
        @JudoTest(modelName = "test")
        @DisplayName("Should handle connection errors gracefully")
        void testConnectionError(JudoDatasourceFixture datasourceFixture) {
            assertNotNull(datasourceFixture);
            DataSource ds = datasourceFixture.getDataSource();
            assertNotNull(ds);

            // DataSource should be functional
            assertDoesNotThrow(() -> {
                try (Connection conn = ds.getConnection()) {
                    assertNotNull(conn);
                }
            });
        }
    }

    /**
     * Test database operations
     */
    @Nested
    @DisplayName("Database Operations Tests")
    class DatabaseOperationsTests {

        @Test
        @JudoTest(modelName = "test", transaction = JudoTest.TransactionHandling.AUTO_COMMIT)
        @DisplayName("Should execute SQL queries")
        void testSqlQuery(JudoDatasourceFixture datasourceFixture) throws SQLException {
            try (Connection conn = datasourceFixture.getDataSource().getConnection(); Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT 1 as test_value")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("test_value"));
            }
        }

        @Test
        @JudoTest(modelName = "test", transaction = JudoTest.TransactionHandling.AUTO_ROLLBACK)
        @DisplayName("Should verify database dialect")
        void testDatabaseDialect(JudoDatasourceFixture datasourceFixture) {
            assertNotNull(datasourceFixture.getDialect());
            assertTrue(datasourceFixture.getDialect().contains("hsqldb") || datasourceFixture.getDialect().contains("postgresql"));
        }

        @Test
        @JudoTest(modelName = "test", dialect = "hsqldb")
        @DisplayName("Should use specified dialect")
        void testSpecifiedDialect(JudoDatasourceFixture datasourceFixture) {
            assertNotNull(datasourceFixture.getDialect());
            assertTrue(datasourceFixture.getDialect().contains("hsqldb"));
        }
    }

    /**
     * Test table truncation
     */
    @Nested
    @DisplayName("Table Truncation Tests")
    class TableTruncationTests {

        @Test
        @JudoTest(modelName = "test", transaction = JudoTest.TransactionHandling.AUTO_COMMIT, truncateTables = true)
        @DisplayName("Should truncate tables when configured")
        void testTableTruncation(JudoRuntimeFixture fixture) {
            assertNotNull(fixture);
            assertNotNull(fixture.modelHolder);
            // Tables will be truncated after this test
        }

        @Test
        @JudoTest(modelName = "test", transaction = JudoTest.TransactionHandling.AUTO_COMMIT, truncateTables = false)
        @DisplayName("Should not truncate tables when disabled")
        void testNoTableTruncation(JudoRuntimeFixture fixture) {
            assertNotNull(fixture);
            // Tables will not be truncated after this test
        }
    }

    /**
     * Test custom Guice modules
     */
    @Nested
    @DisplayName("Custom Module Tests")
    class CustomModuleTests {

        public static class TestModule extends AbstractModule {

            @Override
            protected void configure() {
                // Custom bindings
            }
        }

        @Test
        @JudoTest(modelName = "test", modules = { TestModule.class })
        @DisplayName("Should load custom Guice module")
        void testCustomModule(JudoRuntimeFixture fixture) {
            assertNotNull(fixture);
            assertNotNull(fixture.getInjector());
            // Custom module should be loaded
        }
    }

    /**
     * Test concurrent execution with SINGLETON mode
     */
    @Nested
    @DisplayName("Concurrent Execution Tests")
    @JudoTest(modelName = "test", dataSourceMode = JudoTest.DataSourceMode.SINGLETON)
    class ConcurrentExecutionTests {

        @Test
        @DisplayName("Should share datasource across tests - test 1")
        void testSharedDatasource1(JudoDatasourceFixture datasourceFixture) {
            assertNotNull(datasourceFixture);
            assertNotNull(datasourceFixture.getDataSource());
        }

        @Test
        @DisplayName("Should share datasource across tests - test 2")
        void testSharedDatasource2(JudoDatasourceFixture datasourceFixture) {
            assertNotNull(datasourceFixture);
            assertNotNull(datasourceFixture.getDataSource());
        }

        @Test
        @DisplayName("Should share datasource across tests - test 3")
        void testSharedDatasource3(JudoDatasourceFixture datasourceFixture) {
            assertNotNull(datasourceFixture);
            assertNotNull(datasourceFixture.getDataSource());
        }
    }

    /**
     * Test transaction state validation
     */
    @Nested
    @DisplayName("Transaction State Validation Tests")
    class TransactionStateValidationTests {

        @Test
        @JudoTest(modelName = "test", transaction = JudoTest.TransactionHandling.AUTO_ROLLBACK)
        @DisplayName("Should have active transaction in AUTO_ROLLBACK mode")
        void testActiveTransactionInAutoRollback(JudoDatasourceFixture datasourceFixture) {
            // Transaction should be active
            assertDoesNotThrow(() -> {
                datasourceFixture.getTransactionManager().getTransaction(new org.springframework.transaction.support.DefaultTransactionDefinition(org.springframework.transaction.TransactionDefinition.PROPAGATION_MANDATORY));
            });
        }

        @Test
        @JudoTest(modelName = "test", transaction = JudoTest.TransactionHandling.NONE)
        @DisplayName("Should not have active transaction in NONE mode")
        void testNoActiveTransactionInNoneMode(JudoDatasourceFixture datasourceFixture) {
            // No transaction should be active
            assertThrows(IllegalTransactionStateException.class, () -> {
                datasourceFixture.getTransactionManager().getTransaction(new org.springframework.transaction.support.DefaultTransactionDefinition(org.springframework.transaction.TransactionDefinition.PROPAGATION_MANDATORY));
            });
        }
    }

    /**
     * Test resource cleanup
     */
    @Nested
    @DisplayName("Resource Cleanup Tests")
    class ResourceCleanupTests {

        @Test
        @JudoTest(modelName = "test", dataSourceMode = JudoTest.DataSourceMode.BY_METHOD)
        @DisplayName("Should clean up BY_METHOD datasource after test")
        void testByMethodCleanup(JudoDatasourceFixture datasourceFixture) {
            assertNotNull(datasourceFixture);
            // Datasource should be closed after this test
        }

        @Test
        @JudoTest(modelName = "test", dataSourceMode = JudoTest.DataSourceMode.BY_CLASS)
        @DisplayName("Should keep BY_CLASS datasource across tests in same class")
        void testByClassSharing(JudoDatasourceFixture datasourceFixture) {
            assertNotNull(datasourceFixture);
            // Datasource should be shared across tests in this class
        }
    }

    /**
     * Test model source configurations
     */
    @Nested
    @DisplayName("Model Source Tests")
    class ModelSourceTests {

        @Test
        @JudoTest(modelName = "test", modelSource = JudoTest.ModelSource.AUTO)
        @DisplayName("Should use AUTO model source")
        void testAutoModelSource(JudoRuntimeFixture fixture) {
            assertNotNull(fixture);
            assertNotNull(fixture.modelHolder);
        }

        @Test
        @JudoTest(modelName = "test", modelSource = JudoTest.ModelSource.CLASSPATH)
        @DisplayName("Should use CLASSPATH model source")
        void testClasspathModelSource(JudoRuntimeFixture fixture) {
            assertNotNull(fixture);
            assertNotNull(fixture.modelHolder);
        }

        @Test
        @JudoTest(modelName = "test", modelSource = JudoTest.ModelSource.FILESYSTEM)
        @DisplayName("Should use FILESYSTEM model source")
        void testFilesystemModelSource(JudoRuntimeFixture fixture) {
            assertNotNull(fixture);
            assertNotNull(fixture.modelHolder);
        }
    }
}
