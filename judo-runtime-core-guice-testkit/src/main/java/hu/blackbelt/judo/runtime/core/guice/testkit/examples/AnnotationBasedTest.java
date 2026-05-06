package hu.blackbelt.judo.runtime.core.guice.testkit.examples;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.runtime.core.dispatcher.behaviours.CreateInstanceCall;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoTest;
import hu.blackbelt.judo.runtime.core.guice.testkit.util.ReferenceInjector;

import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Example test class demonstrating the use of @JudoTest annotation.
 * This approach provides the simplest way to test JUDO custom implementations.
 */
@DisplayName("Annotation-based JUDO Tests")
class AnnotationBasedTest {

    /**
     * Simplest possible test - fixture is automatically injected and ready to use
     */
    @JudoTest
    @DisplayName("Simple test with default settings")
    void simpleTest(JudoRuntimeFixture fixture) {
        assertNotNull(fixture);
        assertNotNull(fixture.getInjector());

        // Transaction is automatically started and will be rolled back after test
    }

    /**
     * Test with automatic dependency injection
     */
    @JudoTest
    @DisplayName("Test interceptor with auto-injection")
    void testInterceptorWithAutoInjection(JudoRuntimeFixture fixture) {
        // // One line to create and inject all dependencies
        // UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
        //     UserCreateInterceptor.class,
        //     fixture.getInjector()
        // );

        // assertNotNull(interceptor);
        // // All @Reference fields are now injected and ready to use!
    }

    /**
     * Test with explicit transaction commit
     */
    @JudoTest(transaction = JudoTest.TransactionHandling.AUTO_COMMIT)
    @DisplayName("Test with auto-commit enabled")
    void testWithAutoCommit(JudoRuntimeFixture fixture) {
        // UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
        //     UserCreateInterceptor.class,
        //     fixture.getInjector()
        // );

        // // Prepare test data
        // Payload inputPayload = Payload.map(
        //     "email", "test@example.com",
        //     "form_primaryAddressPostalCode", "1234",
        //     "form_primaryAddressCity", "Budapest",
        //     "form_primaryAddressInformation", "Test St. 1",
        //     "form_primaryPhone", "+36301234567",
        //     "form_primaryPhoneType", "MOBILE"
        // );

        // CreateInstanceCall.CreateInstanceCallPayload payload =
        //     new CreateInstanceCall.CreateInstanceCallPayload(inputPayload);

        // Payload returnPayload = Payload.map("__identifier", 1L);

        // // Execute interceptor
        // Object result = interceptor.postCall(null, payload, returnPayload);

        // assertNotNull(result);

        // // Transaction will be automatically committed after this test
    }

    /**
     * Test without automatic transaction management
     */
    @JudoTest(transaction = JudoTest.TransactionHandling.NONE)
    @DisplayName("Test with manual transaction control")
    void testWithManualTransaction(JudoRuntimeFixture fixture) {
        // UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
        //     UserCreateInterceptor.class,
        //     fixture.getInjector()
        // );

        // // Manually control transaction
        // fixture.beginTransaction();

        // try {
        //     // Your test logic here
        //     assertNotNull(interceptor);

        //     fixture.commitTransaction();
        // } catch (Exception e) {
        //     fixture.rollbackTransaction();
        //     throw e;
        // }
    }

    /**
     * Test with custom model name (if you have multiple models)
     */
    @JudoTest(modelName = "example")
    @DisplayName("Test with custom model name")
    void testWithCustomModel(JudoRuntimeFixture fixture) {
        assertNotNull(fixture);
        assertNotNull(fixture.modelHolder);
    }

    /**
     * Test with PostgreSQL dialect (requires PostgreSQL running)
     */
    @JudoTest(dialect = "postgresql")
    @DisplayName("Test with PostgreSQL dialect")
    void testWithPostgreSQL(JudoRuntimeFixture fixture) {
        assertNotNull(fixture);
        // This test will use PostgreSQL instead of HSQLDB
        // Note: Requires PostgreSQL to be running on localhost:5432
    }

    /**
     * Complete example: Testing user creation with full business logic
     */
    @JudoTest(transaction = JudoTest.TransactionHandling.AUTO_ROLLBACK) // Rollback after test to keep database clean
    @DisplayName("Complete user creation test")
    void completeUserCreationTest(JudoRuntimeFixture fixture) {
        // // 1. Create and inject interceptor
        // UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
        //     UserCreateInterceptor.class,
        //     fixture.getInjector()
        // );

        // // 2. Prepare test data
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

        // // 3. Simulate the framework's return payload
        // Payload returnPayload = Payload.map("__identifier", 1L);

        // // 4. Execute the interceptor's postCall method
        // Object result = interceptor.postCall(null, payload, returnPayload);

        // // 5. Verify results
        // assertNotNull(result, "Interceptor should return a result");

        // // 6. Additional verifications:
        // // - Check that primary address was created
        // // - Check that primary phone was created
        // // - Check that primary email was created
        // // - Check that permissions were calculated

        // // Transaction will be automatically rolled back, keeping database clean
    }

    /**
     * Example showing how to test error handling
     */
    @JudoTest(transaction = JudoTest.TransactionHandling.AUTO_ROLLBACK)
    @DisplayName("Test error handling in interceptor")
    void testErrorHandling(JudoRuntimeFixture fixture) {
        // UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
        //     UserCreateInterceptor.class,
        //     fixture.getInjector()
        // );

        // // Prepare invalid data
        // Payload inputPayload = Payload.map(
        //     "email", "invalid-email" // Invalid email format
        // );

        // CreateInstanceCall.CreateInstanceCallPayload payload =
        //     new CreateInstanceCall.CreateInstanceCallPayload(inputPayload);

        // Payload returnPayload = Payload.map("__identifier", 1L);

        // // Expect exception for invalid data
        // assertThrows(Exception.class, () -> {
        //     interceptor.postCall(null, payload, returnPayload);
        // });

        // // Transaction is automatically rolled back
    }
}
