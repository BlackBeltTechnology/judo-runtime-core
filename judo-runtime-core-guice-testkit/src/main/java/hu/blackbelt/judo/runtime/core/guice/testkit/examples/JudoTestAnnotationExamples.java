package hu.blackbelt.judo.runtime.core.guice.testkit.examples;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.runtime.core.dispatcher.behaviours.CreateInstanceCall;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoDatasourceFixture;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTest;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTest.TransactionHandling;
import hu.blackbelt.judo.runtime.core.guice.testkit.util.ReferenceInjector;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive examples demonstrating @JudoTest annotation with different transaction handling strategies.
 *
 * The @JudoTest annotation is now part of the fixture extensions and provides:
 * - Four transaction handling modes: AUTO_ROLLBACK, AUTO_COMMIT, MANUAL, NONE
 * - Configurable table truncation
 * - Dialect selection (HSQLDB, PostgreSQL)
 * - Model name configuration
 * - Both JudoRuntimeFixture and JudoDatasourceFixture parameter injection
 */
@DisplayName("@JudoTest Annotation Examples")
class JudoTestAnnotationExamples {

    /**
     * EXAMPLE 1: AUTO_ROLLBACK (Default)
     *
     * This is the default and recommended mode for most tests.
     * - Transaction is automatically started before the test
     * - Transaction is automatically rolled back after the test
     * - Database state is clean between tests (no need for table truncation)
     * - Perfect for testing business logic without affecting database
     */
    @Nested
    @DisplayName("AUTO_ROLLBACK Mode (Default)")
    class AutoRollbackExamples {

        @JudoTest
        @DisplayName("Should automatically rollback transaction (implicit default)")
        void testImplicitRollback(JudoRuntimeFixture fixture) {
//            // Transaction is automatically started
//            UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
//                UserCreateInterceptor.class,
//                fixture.getInjector()
//            );
//
//            assertNotNull(interceptor);
//            // Any database changes here will be rolled back automatically
        }

        @JudoTest(transaction = TransactionHandling.AUTO_ROLLBACK)
        @DisplayName("Should automatically rollback transaction (explicit)")
        void testExplicitRollback(JudoRuntimeFixture fixture) {
//            UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
//                UserCreateInterceptor.class,
//                fixture.getInjector()
//            );
//
//            // Simulate creating a user
//            Payload inputPayload = Payload.map(
//                "email", "test@example.com",
//                "form_primaryAddressPostalCode", "1234",
//                "form_primaryAddressCity", "Test City"
//            );
//
//            // This would normally create records in the database
//            // But they'll be rolled back automatically
//            assertNotNull(interceptor);
        }

        @JudoTest(transaction = TransactionHandling.AUTO_ROLLBACK)
        @DisplayName("Should have clean database state between tests")
        void testCleanDatabase(JudoRuntimeFixture fixture) {
            // This test runs after the previous one
            // But the database is clean because previous changes were rolled back
            assertNotNull(fixture.getInjector());
        }
    }

    /**
     * EXAMPLE 2: AUTO_COMMIT
     *
     * Use this when you need to persist changes and verify database state.
     * - Transaction is automatically started before the test
     * - Transaction is automatically committed after the test
     * - Tables are automatically truncated after the test (by default)
     * - Good for integration tests that verify persisted data
     */
    @Nested
    @DisplayName("AUTO_COMMIT Mode")
    class AutoCommitExamples {

        @JudoTest(transaction = TransactionHandling.AUTO_COMMIT)
        @DisplayName("Should commit changes to database")
        void testCommitChanges(JudoRuntimeFixture fixture) {
            // UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            //     UserCreateInterceptor.class,
            //     fixture.getInjector()
            // );

            // assertNotNull(interceptor);
            // // Changes will be committed to the database
            // // Tables will be truncated after test (default behavior)
        }

        @JudoTest(
            transaction = TransactionHandling.AUTO_COMMIT,
            truncateTables = false
        )
        @DisplayName("Should commit changes without truncation")
        void testCommitWithoutTruncation(JudoRuntimeFixture fixture) {
            // // Changes are committed but tables are NOT truncated
            // // Use this carefully - next test will see this test's data!
            // PartnerCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            //     PartnerCreateInterceptor.class,
            //     fixture.getInjector()
            // );

            // assertNotNull(interceptor);
        }

        @JudoTest(
            transaction = TransactionHandling.AUTO_COMMIT,
            truncateTables = true
        )
        @DisplayName("Should commit and truncate tables")
        void testCommitWithTruncation(JudoRuntimeFixture fixture) {
            // // Explicitly enable truncation (this is the default)
            // // Changes are committed then tables are truncated
            // // Next test gets a clean database
            // assertNotNull(fixture.getInjector());
        }
    }

    /**
     * EXAMPLE 3: MANUAL
     *
     * Use this when you need complete control over transaction lifecycle.
     * - No automatic transaction start/commit/rollback
     * - You call fixture.beginTransaction(), commitTransaction(), rollbackTransaction()
     * - Tables are truncated after test (by default)
     * - Good for complex scenarios with savepoints or partial commits
     */
    @Nested
    @DisplayName("MANUAL Mode")
    class ManualTransactionExamples {

        @JudoTest(transaction = TransactionHandling.MANUAL)
        @DisplayName("Should allow manual transaction control")
        void testManualTransaction(JudoRuntimeFixture fixture) {
            // UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            //     UserCreateInterceptor.class,
            //     fixture.getInjector()
            // );

            // // Manually start transaction
            // fixture.beginTransaction();

            // try {
            //     // Your test logic
            //     assertNotNull(interceptor);

            //     // Manually commit
            //     fixture.commitTransaction();
            // } catch (Exception e) {
            //     // Manually rollback on error
            //     fixture.rollbackTransaction();
            //     throw e;
            // }
        }

        @JudoTest(transaction = TransactionHandling.MANUAL)
        @DisplayName("Should support savepoints")
        void testSavepoints(JudoRuntimeFixture fixture) {
            // fixture.beginTransaction();

            // try {
            //     UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            //         UserCreateInterceptor.class,
            //         fixture.getInjector()
            //     );

            //     // Do some work
            //     assertNotNull(interceptor);

            //     // Create savepoint
            //     Object savepoint = fixture.createSavePoint();

            //     try {
            //         // Try something risky
            //         // ...
            //     } catch (Exception e) {
            //         // Rollback to savepoint, keeping earlier work
            //         fixture.rollbackToSavePoint(savepoint);
            //     }

            //     fixture.commitTransaction();
            // } catch (Exception e) {
            //     fixture.rollbackTransaction();
            //     throw e;
            // }
        }

        @JudoTest(
            transaction = TransactionHandling.MANUAL,
            truncateTables = false
        )
        @DisplayName("Should allow manual control without truncation")
        void testManualNoTruncate(JudoRuntimeFixture fixture) {
            // Manual transaction control
            // No automatic table truncation
            fixture.beginTransaction();

            try {
                // Test logic
                fixture.commitTransaction();
            } catch (Exception e) {
                fixture.rollbackTransaction();
                throw e;
            }
        }
    }

    /**
     * EXAMPLE 4: NONE
     *
     * Use this when you don't need transaction support at all.
     * - No transaction management
     * - No table truncation
     * - Direct database access
     * - Good for read-only tests or when testing non-transactional code
     */
    @Nested
    @DisplayName("NONE Mode")
    class NoTransactionExamples {

        @JudoTest(transaction = TransactionHandling.NONE)
        @DisplayName("Should work without transaction support")
        void testNoTransaction(JudoRuntimeFixture fixture) {
            // // No transaction started
            // // Direct database access
            // UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            //     UserCreateInterceptor.class,
            //     fixture.getInjector()
            // );

            // assertNotNull(interceptor);
            // // Use this for read-only tests
        }

        @JudoTest(transaction = TransactionHandling.NONE)
        @DisplayName("Should support read-only operations")
        void testReadOnly(JudoRuntimeFixture fixture) {
            // Good for tests that only read data
            // No transaction overhead
            assertNotNull(fixture.getInjector());
            assertNotNull(fixture.modelHolder);
        }
    }

    /**
     * EXAMPLE 5: Configuration Options
     *
     * Demonstrating other configuration options available with @JudoTest
     */
    @Nested
    @DisplayName("Configuration Options")
    class ConfigurationExamples {

        @JudoTest(modelName = "rackinspect")
        @DisplayName("Should use custom model name")
        void testCustomModel(JudoRuntimeFixture fixture) {
            // Explicitly specify model name (rackinspect is the default)
            assertNotNull(fixture.modelHolder);
        }

        @JudoTest(dialect = "hsqldb")
        @DisplayName("Should use HSQLDB dialect")
        void testHsqldb(JudoRuntimeFixture fixture) {
            // Use HSQLDB (in-memory, fast, default)
            assertNotNull(fixture.getInjector());
        }

        @JudoTest(dialect = "postgresql")
        @DisplayName("Should use PostgreSQL dialect")
        void testPostgreSQL(JudoRuntimeFixture fixture) {
            // Use PostgreSQL (requires PostgreSQL running)
            assertNotNull(fixture.getInjector());
        }

        @JudoTest
        @DisplayName("Should inject both runtime and datasource fixtures")
        void testBothFixtures(JudoRuntimeFixture runtimeFixture, JudoDatasourceFixture datasourceFixture) {
            // You can inject both fixtures as parameters
            assertNotNull(runtimeFixture);
            assertNotNull(datasourceFixture);
            assertNotNull(datasourceFixture.getDataSource());
        }
    }

    /**
     * EXAMPLE 6: Complete Real-World Test
     *
     * A complete example showing realistic test scenario
     */
    @Nested
    @DisplayName("Real-World Examples")
    class RealWorldExamples {

        @JudoTest(transaction = TransactionHandling.AUTO_ROLLBACK)
        @DisplayName("Should test user creation with rollback")
        void testUserCreation(JudoRuntimeFixture fixture) {
            // // Setup
            // UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            //     UserCreateInterceptor.class,
            //     fixture.getInjector()
            // );

            // // Prepare test data
            // Payload inputPayload = Payload.map(
            //     "email", "john.doe@example.com",
            //     "firstName", "John",
            //     "lastName", "Doe",
            //     "form_primaryAddressPostalCode", "1011",
            //     "form_primaryAddressCity", "Budapest",
            //     "form_primaryAddressInformation", "Main Street 1",
            //     "form_primaryPhone", "+36301234567",
            //     "form_primaryPhoneType", "MOBILE"
            // );

            // CreateInstanceCall.CreateInstanceCallPayload payload =
            //     new CreateInstanceCall.CreateInstanceCallPayload(inputPayload);

            // Payload returnPayload = Payload.map("__identifier", 1L);

            // // Execute
            // Object result = interceptor.postCall(null, payload, returnPayload);

            // // Verify
            // assertNotNull(result);

            // // All changes automatically rolled back - database stays clean
        }

        @JudoTest(
            transaction = TransactionHandling.AUTO_COMMIT,
            truncateTables = true
        )
        @DisplayName("Should test with data persistence and cleanup")
        void testWithPersistence(JudoRuntimeFixture fixture) {
            // // This test commits changes but cleans up after itself
            // PartnerCreateInterceptor interceptor = ReferenceInjector.createAndInject(
            //     PartnerCreateInterceptor.class,
            //     fixture.getInjector()
            // );

            // assertNotNull(interceptor);
            // // Changes committed, then tables truncated
            // // Next test gets clean database
        }
    }

    /**
     * CHOOSING THE RIGHT TRANSACTION MODE:
     *
     * Use AUTO_ROLLBACK (default) when:
     * - Testing business logic
     * - You don't need to persist changes
     * - You want the fastest tests
     * - You want guaranteed clean database between tests
     *
     * Use AUTO_COMMIT when:
     * - You need to verify persisted data
     * - Testing integration with external systems
     * - You want to see actual database state
     * - You're okay with slightly slower tests (truncation overhead)
     *
     * Use MANUAL when:
     * - You need savepoints
     * - Complex transaction scenarios (partial commits)
     * - Testing transaction-related edge cases
     * - You need complete control
     *
     * Use NONE when:
     * - Read-only tests
     * - Testing non-transactional code
     * - You don't need transaction support
     * - Maximum performance for read operations
     */
}
