package hu.blackbelt.judo.runtime.core.guice.testkit.util;

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptor;
import org.eclipse.emf.ecore.EOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link TestOperationCallInterceptorProvider}.
 */
class TestOperationCallInterceptorProviderTest {

    private TestOperationCallInterceptorProvider provider;

    @BeforeEach
    void setUp() {
        provider = new TestOperationCallInterceptorProvider();
    }

    // ==========================================
    // Positive Tests
    // ==========================================

    @Test
    void testAddSingleInterceptor() {
        // Given
        OperationCallInterceptor interceptor = new TestInterceptor("test1");

        // When
        provider.addInterceptor(interceptor);

        // Then
        assertEquals(1, provider.size());
        assertFalse(provider.isEmpty());
        assertTrue(provider.getInterceptors().contains(interceptor));
        assertTrue(provider.getCallOperationInterceptors().contains(interceptor));
    }

    @Test
    void testAddMultipleInterceptorsPreservesOrder() {
        // Given
        OperationCallInterceptor interceptor1 = new TestInterceptor("first");
        OperationCallInterceptor interceptor2 = new TestInterceptor("second");
        OperationCallInterceptor interceptor3 = new TestInterceptor("third");

        // When
        provider.addInterceptor(interceptor1);
        provider.addInterceptor(interceptor2);
        provider.addInterceptor(interceptor3);

        // Then
        assertEquals(3, provider.size());
        List<OperationCallInterceptor> interceptors = provider.getInterceptors();
        assertEquals("first", interceptors.get(0).getName());
        assertEquals("second", interceptors.get(1).getName());
        assertEquals("third", interceptors.get(2).getName());
    }

    @Test
    void testRemoveInterceptor() {
        // Given
        OperationCallInterceptor interceptor1 = new TestInterceptor("keep");
        OperationCallInterceptor interceptor2 = new TestInterceptor("remove");
        provider.addInterceptor(interceptor1);
        provider.addInterceptor(interceptor2);

        // When
        boolean removed = provider.removeInterceptor(interceptor2);

        // Then
        assertTrue(removed);
        assertEquals(1, provider.size());
        assertTrue(provider.getInterceptors().contains(interceptor1));
        assertFalse(provider.getInterceptors().contains(interceptor2));
    }

    @Test
    void testClearInterceptors() {
        // Given
        provider.addInterceptor(new TestInterceptor("one"));
        provider.addInterceptor(new TestInterceptor("two"));
        provider.addInterceptor(new TestInterceptor("three"));
        assertEquals(3, provider.size());

        // When
        provider.clearInterceptors();

        // Then
        assertEquals(0, provider.size());
        assertTrue(provider.isEmpty());
        assertTrue(provider.getInterceptors().isEmpty());
    }

    @Test
    void testGetInterceptorsReturnsDefensiveCopy() {
        // Given
        OperationCallInterceptor interceptor = new TestInterceptor("original");
        provider.addInterceptor(interceptor);

        // When
        List<OperationCallInterceptor> copy = provider.getInterceptors();
        copy.clear(); // Modify the returned list

        // Then - original provider should be unaffected
        assertEquals(1, provider.size());
        assertTrue(provider.getInterceptors().contains(interceptor));
    }

    @Test
    void testGetCallOperationInterceptorsReturnsDefensiveCopy() {
        // Given
        OperationCallInterceptor interceptor = new TestInterceptor("original");
        provider.addInterceptor(interceptor);

        // When
        Collection<OperationCallInterceptor> copy = provider.getCallOperationInterceptors();
        copy.clear(); // Modify the returned collection

        // Then - original provider should be unaffected
        assertEquals(1, provider.size());
    }

    @Test
    void testIsEmptyOnNewProvider() {
        // Then
        assertTrue(provider.isEmpty());
        assertEquals(0, provider.size());
    }

    @Test
    void testSizeAfterMultipleOperations() {
        // Given
        OperationCallInterceptor i1 = new TestInterceptor("1");
        OperationCallInterceptor i2 = new TestInterceptor("2");
        OperationCallInterceptor i3 = new TestInterceptor("3");

        // When/Then
        provider.addInterceptor(i1);
        assertEquals(1, provider.size());

        provider.addInterceptor(i2);
        assertEquals(2, provider.size());

        provider.addInterceptor(i3);
        assertEquals(3, provider.size());

        provider.removeInterceptor(i2);
        assertEquals(2, provider.size());

        provider.clearInterceptors();
        assertEquals(0, provider.size());
    }

    // ==========================================
    // Negative Tests
    // ==========================================

    @Test
    void testAddNullInterceptorThrowsException() {
        // When/Then
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> provider.addInterceptor(null)
        );
        assertEquals("Interceptor cannot be null", exception.getMessage());
    }

    @Test
    void testRemoveNonExistentInterceptorReturnsFalse() {
        // Given
        OperationCallInterceptor registered = new TestInterceptor("registered");
        OperationCallInterceptor notRegistered = new TestInterceptor("notRegistered");
        provider.addInterceptor(registered);

        // When
        boolean result = provider.removeInterceptor(notRegistered);

        // Then
        assertFalse(result);
        assertEquals(1, provider.size()); // Original still there
    }

    @Test
    void testRemoveFromEmptyProviderReturnsFalse() {
        // Given - empty provider

        // When
        boolean result = provider.removeInterceptor(new TestInterceptor("any"));

        // Then
        assertFalse(result);
    }

    // ==========================================
    // Test Helper: Simple Interceptor Implementation
    // ==========================================

    /**
     * Simple test interceptor for unit testing.
     */
    private static class TestInterceptor implements OperationCallInterceptor {
        private final String name;

        TestInterceptor(String name) {
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
}
