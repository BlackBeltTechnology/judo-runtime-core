package hu.blackbelt.judo.runtime.core.guice.testkit.fixture;

import com.google.inject.AbstractModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptor;
import hu.blackbelt.judo.runtime.core.guice.testkit.util.TestOperationCallInterceptorProvider;
import org.eclipse.emf.ecore.EOperation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoDatasourceFixture.DIALECT_HSQLDB;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for interceptor support in {@link JudoRuntimeFixture}.
 *
 * <p>Tests cover:
 * <ul>
 * <li>Positive cases: registering interceptors by class and instance</li>
 * <li>Negative cases: null inputs, wrong timing, missing constructors</li>
 * <li>Combination cases: multiple interceptors, runtime modifications</li>
 * </ul>
 */
@Disabled("Requires a real JUDO model 'exa' which doesn't exist in test environment")
class JudoRuntimeFixtureInterceptorTest {

    private DataSource dataSource;
    private JudoRuntimeFixture fixture;

    @BeforeEach
    void setUp() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:hsqldb:mem:interceptortest;shutdown=true");
        config.setUsername("SA");
        config.setPassword("");
        config.setMaximumPoolSize(5);
        dataSource = new HikariDataSource(config);
    }

    @AfterEach
    void tearDown() {
        if (fixture != null) {
            fixture.tearDown();
        }
        if (dataSource instanceof HikariDataSource) {
            ((HikariDataSource) dataSource).close();
        }
    }

    // ==========================================
    // Positive Tests
    // ==========================================

    @Test
    void testRegisterInterceptorByClassBeforeInit() throws Exception {
        // Given
        fixture = new JudoRuntimeFixture();
        fixture.prepare("exa", dataSource, DIALECT_HSQLDB);

        // When
        fixture.addInterceptor(SimpleTestInterceptor.class);
        fixture.init(new AbstractModule() {}, null);

        // Then
        TestOperationCallInterceptorProvider provider = fixture.getInterceptorProvider();
        assertNotNull(provider);
        assertEquals(1, provider.size());
        assertTrue(provider.getInterceptors().get(0) instanceof SimpleTestInterceptor);
    }

    @Test
    void testRegisterInterceptorByInstanceBeforeInit() throws Exception {
        // Given
        fixture = new JudoRuntimeFixture();
        fixture.prepare("exa", dataSource, DIALECT_HSQLDB);
        SimpleTestInterceptor interceptor = new SimpleTestInterceptor();

        // When
        fixture.addInterceptor(interceptor);
        fixture.init(new AbstractModule() {}, null);

        // Then
        TestOperationCallInterceptorProvider provider = fixture.getInterceptorProvider();
        assertEquals(1, provider.size());
        assertSame(interceptor, provider.getInterceptors().get(0));
    }

    @Test
    void testRegisterMultipleInterceptorsMixedClassAndInstance() throws Exception {
        // Given
        fixture = new JudoRuntimeFixture();
        fixture.prepare("exa", dataSource, DIALECT_HSQLDB);
        SimpleTestInterceptor instanceInterceptor = new SimpleTestInterceptor();
        instanceInterceptor.setName("instance");

        // When
        fixture.addInterceptor(SimpleTestInterceptor.class); // by class
        fixture.addInterceptor(instanceInterceptor); // by instance
        fixture.addInterceptor(AnotherTestInterceptor.class); // another class
        fixture.init(new AbstractModule() {}, null);

        // Then
        TestOperationCallInterceptorProvider provider = fixture.getInterceptorProvider();
        assertEquals(3, provider.size());

        List<OperationCallInterceptor> interceptors = provider.getInterceptors();
        assertTrue(interceptors.get(0) instanceof SimpleTestInterceptor);
        assertSame(instanceInterceptor, interceptors.get(1));
        assertTrue(interceptors.get(2) instanceof AnotherTestInterceptor);
    }

    @Test
    void testGetInterceptorProviderAfterInit() throws Exception {
        // Given
        fixture = new JudoRuntimeFixture();
        fixture.prepare("exa", dataSource, DIALECT_HSQLDB);
        fixture.addInterceptor(SimpleTestInterceptor.class);
        fixture.init(new AbstractModule() {}, null);

        // When
        TestOperationCallInterceptorProvider provider = fixture.getInterceptorProvider();

        // Then
        assertNotNull(provider);
        assertEquals(1, provider.size());
    }

    @Test
    void testInterceptorProviderCanBeModifiedAfterInit() throws Exception {
        // Given
        fixture = new JudoRuntimeFixture();
        fixture.prepare("exa", dataSource, DIALECT_HSQLDB);
        fixture.addInterceptor(SimpleTestInterceptor.class);
        fixture.init(new AbstractModule() {}, null);

        TestOperationCallInterceptorProvider provider = fixture.getInterceptorProvider();
        assertEquals(1, provider.size());

        // When - add another interceptor via provider
        AnotherTestInterceptor newInterceptor = new AnotherTestInterceptor();
        provider.addInterceptor(newInterceptor);

        // Then
        assertEquals(2, provider.size());
        assertTrue(provider.getInterceptors().contains(newInterceptor));
    }

    // ==========================================
    // Negative Tests
    // ==========================================

    @Test
    void testAddInterceptorClassAfterInitThrowsException() throws Exception {
        // Given
        fixture = new JudoRuntimeFixture();
        fixture.prepare("exa", dataSource, DIALECT_HSQLDB);
        fixture.init(new AbstractModule() {}, null);

        // When/Then
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> fixture.addInterceptor(SimpleTestInterceptor.class)
        );
        assertTrue(exception.getMessage().contains("after init()"));
    }

    @Test
    void testAddInterceptorInstanceAfterInitThrowsException() throws Exception {
        // Given
        fixture = new JudoRuntimeFixture();
        fixture.prepare("exa", dataSource, DIALECT_HSQLDB);
        fixture.init(new AbstractModule() {}, null);

        // When/Then
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> fixture.addInterceptor(new SimpleTestInterceptor())
        );
        assertTrue(exception.getMessage().contains("after init()"));
    }

    @Test
    void testAddNullInterceptorClassThrowsException() throws Exception {
        // Given
        fixture = new JudoRuntimeFixture();
        fixture.prepare("exa", dataSource, DIALECT_HSQLDB);

        // When/Then
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> fixture.addInterceptor((Class<? extends OperationCallInterceptor>) null)
        );
        assertTrue(exception.getMessage().contains("null"));
    }

    @Test
    void testAddNullInterceptorInstanceThrowsException() throws Exception {
        // Given
        fixture = new JudoRuntimeFixture();
        fixture.prepare("exa", dataSource, DIALECT_HSQLDB);

        // When/Then
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> fixture.addInterceptor((OperationCallInterceptor) null)
        );
        assertTrue(exception.getMessage().contains("null"));
    }

    @Test
    void testAddInterceptorWithoutNoArgConstructorFails() throws Exception {
        // Given
        fixture = new JudoRuntimeFixture();
        fixture.prepare("exa", dataSource, DIALECT_HSQLDB);
        fixture.addInterceptor(InterceptorWithoutNoArgConstructor.class);

        // When/Then
        RuntimeException exception = assertThrows(
            RuntimeException.class,
            () -> fixture.init(new AbstractModule() {}, null)
        );
        assertTrue(exception.getMessage().contains("no-arg constructor") ||
                   exception.getMessage().contains("NoSuchMethodException") ||
                   exception.getCause() instanceof NoSuchMethodException);
    }

    @Test
    void testGetInterceptorProviderBeforeInitThrowsException() throws Exception {
        // Given
        fixture = new JudoRuntimeFixture();
        fixture.prepare("exa", dataSource, DIALECT_HSQLDB);
        // Note: init() not called

        // When/Then
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> fixture.getInterceptorProvider()
        );
        assertTrue(exception.getMessage().contains("init()"));
    }

    // ==========================================
    // Combination Tests
    // ==========================================

    @Test
    void testClearInterceptorsMidTest() throws Exception {
        // Given
        fixture = new JudoRuntimeFixture();
        fixture.prepare("exa", dataSource, DIALECT_HSQLDB);
        fixture.addInterceptor(SimpleTestInterceptor.class);
        fixture.addInterceptor(AnotherTestInterceptor.class);
        fixture.init(new AbstractModule() {}, null);

        TestOperationCallInterceptorProvider provider = fixture.getInterceptorProvider();
        assertEquals(2, provider.size());

        // When
        provider.clearInterceptors();

        // Then
        assertEquals(0, provider.size());
        assertTrue(provider.isEmpty());
    }

    @Test
    void testMultipleFixturesHaveIndependentProviders() throws Exception {
        // Given
        JudoRuntimeFixture fixture1 = new JudoRuntimeFixture();
        fixture1.prepare("exa", dataSource, DIALECT_HSQLDB);
        fixture1.addInterceptor(SimpleTestInterceptor.class);
        fixture1.init(new AbstractModule() {}, null);

        // Create second datasource for second fixture
        HikariConfig config2 = new HikariConfig();
        config2.setJdbcUrl("jdbc:hsqldb:mem:interceptortest2;shutdown=true");
        config2.setUsername("SA");
        config2.setPassword("");
        config2.setMaximumPoolSize(5);
        DataSource dataSource2 = new HikariDataSource(config2);

        JudoRuntimeFixture fixture2 = new JudoRuntimeFixture();
        fixture2.prepare("exa", dataSource2, DIALECT_HSQLDB);
        fixture2.addInterceptor(AnotherTestInterceptor.class);
        fixture2.addInterceptor(SimpleTestInterceptor.class);
        fixture2.init(new AbstractModule() {}, null);

        try {
            // When/Then
            assertEquals(1, fixture1.getInterceptorProvider().size());
            assertEquals(2, fixture2.getInterceptorProvider().size());

            // Modifying one doesn't affect the other
            fixture1.getInterceptorProvider().clearInterceptors();
            assertEquals(0, fixture1.getInterceptorProvider().size());
            assertEquals(2, fixture2.getInterceptorProvider().size());
        } finally {
            fixture1.tearDown();
            fixture2.tearDown();
            ((HikariDataSource) dataSource2).close();
        }

        // Set fixture to null to prevent double tearDown in @AfterEach
        fixture = null;
    }

    @Test
    void testNoInterceptorsRegisteredResultsInEmptyProvider() throws Exception {
        // Given
        fixture = new JudoRuntimeFixture();
        fixture.prepare("exa", dataSource, DIALECT_HSQLDB);
        // Note: no interceptors added

        // When
        fixture.init(new AbstractModule() {}, null);

        // Then
        TestOperationCallInterceptorProvider provider = fixture.getInterceptorProvider();
        assertNotNull(provider);
        assertEquals(0, provider.size());
        assertTrue(provider.isEmpty());
    }

    // ==========================================
    // Test Interceptor Implementations
    // ==========================================

    /**
     * Simple test interceptor with no-arg constructor.
     */
    public static class SimpleTestInterceptor implements OperationCallInterceptor {
        private String name = "SimpleTestInterceptor";

        public SimpleTestInterceptor() {
            // Required no-arg constructor
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Collection<EOperation> getOperations(AsmModel asmModel) {
            return Collections.emptyList();
        }
    }

    /**
     * Another test interceptor for testing multiple interceptors.
     */
    public static class AnotherTestInterceptor implements OperationCallInterceptor {
        public AnotherTestInterceptor() {
            // Required no-arg constructor
        }

        @Override
        public String getName() {
            return "AnotherTestInterceptor";
        }

        @Override
        public Collection<EOperation> getOperations(AsmModel asmModel) {
            return Collections.emptyList();
        }
    }

    /**
     * Interceptor without no-arg constructor for negative testing.
     */
    public static class InterceptorWithoutNoArgConstructor implements OperationCallInterceptor {
        private final String requiredParam;

        // Only constructor requires a parameter - no no-arg constructor
        public InterceptorWithoutNoArgConstructor(String requiredParam) {
            this.requiredParam = requiredParam;
        }

        @Override
        public String getName() {
            return "InterceptorWithoutNoArgConstructor-" + requiredParam;
        }

        @Override
        public Collection<EOperation> getOperations(AsmModel asmModel) {
            return Collections.emptyList();
        }
    }
}
