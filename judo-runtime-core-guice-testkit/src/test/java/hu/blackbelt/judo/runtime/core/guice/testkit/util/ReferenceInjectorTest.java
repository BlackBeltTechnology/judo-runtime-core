package hu.blackbelt.judo.runtime.core.guice.testkit.util;

import static org.junit.jupiter.api.Assertions.*;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.osgi.service.component.annotations.Reference;

/**
 * Test class demonstrating usage of ReferenceInjector for testing custom implementations.
 */
class ReferenceInjectorTest {

    private Injector injector;

    // Example service interfaces
    interface UserDao {
        String getName();
    }

    interface PartnerDao {
        String getName();
    }

    // Example service implementations
    static class UserDaoImpl implements UserDao {
        @Override
        public String getName() {
            return "UserDao";
        }
    }

    static class PartnerDaoImpl implements PartnerDao {
        @Override
        public String getName() {
            return "PartnerDao";
        }
    }

    // Example custom implementation with @Reference fields
    static class SampleInterceptorWithFields {
        @Reference
        UserDao userDao;

        @Reference
        PartnerDao partnerDao;

        public boolean hasInjectedDependencies() {
            return userDao != null && partnerDao != null;
        }

        public String getUserDaoName() {
            return userDao.getName();
        }

        public String getPartnerDaoName() {
            return partnerDao.getName();
        }
    }

    // Example custom implementation with @Reference setters
    static class SampleInterceptorWithSetters {
        private UserDao userDao;
        private PartnerDao partnerDao;

        @Reference
        public void setUserDao(UserDao userDao) {
            this.userDao = userDao;
        }

        @Reference
        public void setPartnerDao(PartnerDao partnerDao) {
            this.partnerDao = partnerDao;
        }

        public boolean hasInjectedDependencies() {
            return userDao != null && partnerDao != null;
        }

        public String getUserDaoName() {
            return userDao.getName();
        }

        public String getPartnerDaoName() {
            return partnerDao.getName();
        }
    }

    // Example with mixed fields and setters
    static class SampleInterceptorMixed {
        @Reference
        UserDao userDao;

        private PartnerDao partnerDao;

        @Reference
        public void setPartnerDao(PartnerDao partnerDao) {
            this.partnerDao = partnerDao;
        }

        public boolean hasInjectedDependencies() {
            return userDao != null && partnerDao != null;
        }
    }

    @BeforeEach
    void setUp() {
        // Create a simple Guice module with test dependencies
        injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(UserDao.class).to(UserDaoImpl.class);
                bind(PartnerDao.class).to(PartnerDaoImpl.class);
            }
        });
    }

    @Test
    void testInjectReferencesWithFields() {
        // Create instance
        SampleInterceptorWithFields interceptor = new SampleInterceptorWithFields();
        
        // Initially dependencies are null
        assertFalse(interceptor.hasInjectedDependencies());
        
        // Inject references
        ReferenceInjector.injectReferences(interceptor, injector);
        
        // Verify dependencies were injected
        assertTrue(interceptor.hasInjectedDependencies());
        assertEquals("UserDao", interceptor.getUserDaoName());
        assertEquals("PartnerDao", interceptor.getPartnerDaoName());
    }

    @Test
    void testInjectReferencesWithSetters() {
        // Create instance
        SampleInterceptorWithSetters interceptor = new SampleInterceptorWithSetters();
        
        // Initially dependencies are null
        assertFalse(interceptor.hasInjectedDependencies());
        
        // Inject references
        ReferenceInjector.injectReferences(interceptor, injector);
        
        // Verify dependencies were injected via setters
        assertTrue(interceptor.hasInjectedDependencies());
        assertEquals("UserDao", interceptor.getUserDaoName());
        assertEquals("PartnerDao", interceptor.getPartnerDaoName());
    }

    @Test
    void testInjectReferencesWithMixedFieldsAndSetters() {
        // Create instance
        SampleInterceptorMixed interceptor = new SampleInterceptorMixed();
        
        // Initially dependencies are null
        assertFalse(interceptor.hasInjectedDependencies());
        
        // Inject references
        ReferenceInjector.injectReferences(interceptor, injector);
        
        // Verify both field and setter injections worked
        assertTrue(interceptor.hasInjectedDependencies());
    }

    @Test
    void testCreateAndInject() {
        // Create and inject in one call
        SampleInterceptorWithFields interceptor = ReferenceInjector.createAndInject(
            SampleInterceptorWithFields.class, 
            injector
        );
        
        // Verify instance was created and dependencies were injected
        assertNotNull(interceptor);
        assertTrue(interceptor.hasInjectedDependencies());
        assertEquals("UserDao", interceptor.getUserDaoName());
        assertEquals("PartnerDao", interceptor.getPartnerDaoName());
    }

    @Test
    void testInjectReferencesThrowsExceptionWhenTargetIsNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            ReferenceInjector.injectReferences(null, injector);
        });
    }

    @Test
    void testInjectReferencesThrowsExceptionWhenInjectorIsNull() {
        SampleInterceptorWithFields interceptor = new SampleInterceptorWithFields();
        
        assertThrows(IllegalArgumentException.class, () -> {
            ReferenceInjector.injectReferences(interceptor, null);
        });
    }
}
