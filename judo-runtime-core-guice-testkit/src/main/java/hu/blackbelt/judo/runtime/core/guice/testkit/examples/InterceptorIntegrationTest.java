package hu.blackbelt.judo.runtime.core.guice.testkit.examples;

import com.google.inject.AbstractModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import static hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoDatasourceFixture.DIALECT_HSQLDB;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Example integration test demonstrating how to test custom interceptors
 * using ReferenceInjector with the JUDO testkit framework.
 *
 * This example shows how to:
 * 1. Set up JudoRuntimeFixture with a test database
 * 2. Create an interceptor instance
 * 3. Use ReferenceInjector to automatically inject all @Reference dependencies
 * 4. Test the interceptor logic
 */
class InterceptorIntegrationTest {

    private DataSource dataSource;
    private JudoRuntimeFixture runtimeFixture;

    @BeforeEach
    void setUp() throws Exception {
        // Initialize HSQLDB in-memory datasource
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:hsqldb:mem:test");
        config.setUsername("SA");
        config.setPassword("");
        config.setMaximumPoolSize(10);
        dataSource = new HikariDataSource(config);

        // Initialize runtime fixture
        runtimeFixture = new JudoRuntimeFixture();
        runtimeFixture.prepare("exa", dataSource, DIALECT_HSQLDB);

        // Initialize with custom module (if needed)
        runtimeFixture.init(new AbstractModule() {
            @Override
            protected void configure() {
                // Add any custom bindings here if needed
            }
        }, null);
    }

    @AfterEach
    void tearDown() {
        if (runtimeFixture != null) {
            runtimeFixture.tearDown();
        }
        if (dataSource instanceof HikariDataSource) {
            ((HikariDataSource) dataSource).close();
        }
    }

    @Test
    void testInterceptorWithAutomaticReferenceInjection() {
        // This test demonstrates the usage pattern for testing any custom implementation
        // Replace with your actual interceptor class:
        //
        // Example:
        // UserCreateInterceptor interceptor = new UserCreateInterceptor();
        // ReferenceInjector.injectReferences(interceptor, runtimeFixture.getInjector());
        //
        // Now all @Reference annotated fields (userDao, recalculatePermissions, etc.)
        // are automatically injected and ready to use
        //
        // You can then:
        // 1. Start a transaction
        // 2. Call interceptor methods (preCall, postCall)
        // 3. Verify business logic
        // 4. Commit or rollback

        // Start transaction
        runtimeFixture.beginTransaction();

        try {
            // Your test logic here
            // Example:
            // - Create test data
            // - Call interceptor.preCall() or interceptor.postCall()
            // - Verify expected behavior
            // - Query database to verify changes

            // Commit transaction
            runtimeFixture.commitTransaction();
        } catch (Exception e) {
            runtimeFixture.rollbackTransaction();
            throw e;
        }
    }

    @Test
    void testCreateAndInjectPattern() {
        // Alternative pattern: create and inject in one call
        // This is useful when you don't need to customize the interceptor instance
        // before injection
        //
        // Example:
        // UserCreateInterceptor interceptor = ReferenceInjector.createAndInject(
        //     UserCreateInterceptor.class,
        //     runtimeFixture.getInjector()
        // );
        //
        // All @Reference dependencies are now injected

        assertNotNull(runtimeFixture.getInjector());
    }

    /**
     * Example test pattern for field-based @Reference injection
     */
    @Test
    void testFieldBasedReferenceInjection() {
        // For interceptors with @Reference fields like:
        //
        // @Reference
        // UserDao userDao;
        //
        // @Reference
        // RecalculatePermissions recalculatePermissions;
        //
        // Usage:
        // UserCreateInterceptor interceptor = new UserCreateInterceptor();
        // ReferenceInjector.injectReferences(interceptor, runtimeFixture.getInjector());
        //
        // Now interceptor.userDao and interceptor.recalculatePermissions are ready to use
    }

    /**
     * Example test pattern for setter-based @Reference injection
     */
    @Test
    void testSetterBasedReferenceInjection() {
        // For custom operations with @Reference setters like:
        //
        // @Reference
        // public void setUserDao(UserDao userDao) {
        //     this.userDao = userDao;
        // }
        //
        // Usage is identical:
        // MyCustomOperation operation = new MyCustomOperation();
        // ReferenceInjector.injectReferences(operation, runtimeFixture.getInjector());
        //
        // The setter will be called automatically
    }

    /**
     * Example test pattern for testing with transactions
     */
    @Test
    void testWithTransactionManagement() {
        // Pattern for testing interceptor behavior within transactions:
        //
        // 1. Begin transaction
        runtimeFixture.beginTransaction();

        try {
            // 2. Create savepoint if needed for partial rollback
            Object savepoint = runtimeFixture.createSavePoint();

            // 3. Execute your test logic
            // interceptor.postCall(operation, parameters, result);

            // 4. Rollback to savepoint if needed
            // runtimeFixture.rollbackToSavePoint(savepoint);

            // 5. Commit if everything is ok
            runtimeFixture.commitTransaction();

        } catch (Exception e) {
            // 6. Rollback on error
            runtimeFixture.rollbackTransaction();
            throw e;
        }
    }
}
