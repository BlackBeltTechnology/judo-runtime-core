package hu.blackbelt.judo.runtime.core.guice.testkit.util;

import com.google.inject.Injector;
import com.google.inject.ConfigurationException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for automatically injecting dependencies into custom implementations in test scenarios.
 * This allows custom implementations (interceptors, operations, etc.) to be tested easily without OSGi container.
 * 
 * Note: OSGi's @Reference annotation has CLASS retention (not RUNTIME), so it's not available via reflection.
 * Instead, this utility injects all non-static, non-final fields that have matching types in the Guice injector.
 * 
 * Usage:
 * <pre>
 * {@code
 * ReferenceInjector.injectReferences(interceptorInstance, injector);
 * }
 * </pre>
 */
public class ReferenceInjector {

    /**
     * Injects all injectable fields and setters in the target object using the provided Guice injector.
     * 
     * For fields: Injects all non-static, non-final, package/protected/public fields where the type
     * is available in the injector.
     * 
     * For setters: Injects via setter methods named "set*" with a single parameter where the type
     * is available in the injector.
     * 
     * @param target The object to inject references into (e.g., interceptor, custom operation)
     * @param injector The Guice injector containing the dependencies
     * @throws IllegalStateException if injection fails
     */
    public static void injectReferences(Object target, Injector injector) {
        if (target == null) {
            throw new IllegalArgumentException("Target object cannot be null");
        }
        if (injector == null) {
            throw new IllegalArgumentException("Injector cannot be null");
        }

        Class<?> targetClass = target.getClass();

        // Inject injectable fields
        injectFields(target, targetClass, injector);

        // Inject via setter methods
        injectSetters(target, targetClass, injector);
    }

    /**
     * Injects all injectable fields in the target object.
     * Fields are injectable if they are non-static, non-final, and the type is available in the injector.
     */
    private static void injectFields(Object target, Class<?> targetClass, Injector injector) {
        List<Field> injectableFields = findInjectableFields(targetClass);
        
        for (Field field : injectableFields) {
            try {
                field.setAccessible(true);
                Class<?> fieldType = field.getType();
                
                // Try to get instance from Guice injector
                Object dependency = getInstanceIfAvailable(injector, fieldType);
                
                if (dependency != null) {
                    field.set(target, dependency);
                }
                
            } catch (Exception e) {
                throw new IllegalStateException(
                    "Failed to inject field: " + field.getName() + 
                    " in class: " + targetClass.getName(), e
                );
            }
        }
    }

    /**
     * Injects via setter methods in the target object.
     * Setters are injectable if they start with "set", have exactly one parameter,
     * and the parameter type is available in the injector.
     */
    private static void injectSetters(Object target, Class<?> targetClass, Injector injector) {
        List<Method> injectableSetters = findInjectableSetters(targetClass);
        
        for (Method setter : injectableSetters) {
            try {
                setter.setAccessible(true);
                
                // Setter should have exactly one parameter
                if (setter.getParameterCount() != 1) {
                    continue;
                }
                
                Class<?> parameterType = setter.getParameterTypes()[0];
                
                // Try to get instance from Guice injector
                Object dependency = getInstanceIfAvailable(injector, parameterType);
                
                if (dependency != null) {
                    setter.invoke(target, dependency);
                }
                
            } catch (Exception e) {
                throw new IllegalStateException(
                    "Failed to inject via setter: " + setter.getName() + 
                    " in class: " + targetClass.getName(), e
                );
            }
        }
    }

    /**
     * Finds all injectable fields in the class hierarchy.
     * Fields are injectable if they are non-static, non-final, and not primitive.
     */
    private static List<Field> findInjectableFields(Class<?> clazz) {
        List<Field> injectableFields = new ArrayList<>();
        
        Class<?> currentClass = clazz;
        while (currentClass != null && currentClass != Object.class) {
            Field[] declaredFields = currentClass.getDeclaredFields();
            
            for (Field field : declaredFields) {
                int modifiers = field.getModifiers();
                
                // Skip static and final fields
                if (Modifier.isStatic(modifiers) || Modifier.isFinal(modifiers)) {
                    continue;
                }
                
                // Skip primitive types
                if (field.getType().isPrimitive()) {
                    continue;
                }
                
                injectableFields.add(field);
            }
            
            currentClass = currentClass.getSuperclass();
        }
        
        return injectableFields;
    }

    /**
     * Finds all injectable setter methods in the class hierarchy.
     * Setters must start with "set", have exactly one parameter, and be non-static.
     */
    private static List<Method> findInjectableSetters(Class<?> clazz) {
        List<Method> injectableSetters = new ArrayList<>();
        
        Class<?> currentClass = clazz;
        while (currentClass != null && currentClass != Object.class) {
            Method[] declaredMethods = currentClass.getDeclaredMethods();
            
            for (Method method : declaredMethods) {
                // Must be a setter (starts with "set")
                if (!method.getName().startsWith("set")) {
                    continue;
                }
                
                // Must not be static
                if (Modifier.isStatic(method.getModifiers())) {
                    continue;
                }
                
                // Must have exactly one parameter
                if (method.getParameterCount() != 1) {
                    continue;
                }
                
                // Skip primitive parameters
                if (method.getParameterTypes()[0].isPrimitive()) {
                    continue;
                }
                
                injectableSetters.add(method);
            }
            
            currentClass = currentClass.getSuperclass();
        }
        
        return injectableSetters;
    }

    /**
     * Tries to get an instance from the injector. Returns null if the type is not bound.
     */
    private static Object getInstanceIfAvailable(Injector injector, Class<?> type) {
        try {
            return injector.getInstance(type);
        } catch (ConfigurationException e) {
            // Type is not bound in the injector, skip it
            return null;
        }
    }

    /**
     * Convenience method to create and inject references into a new instance of the target class.
     * 
     * @param targetClass The class to instantiate
     * @param injector The Guice injector containing the dependencies
     * @param <T> The type of the target class
     * @return A new instance with all @Reference dependencies injected
     * @throws IllegalStateException if instantiation or injection fails
     */
    public static <T> T createAndInject(Class<T> targetClass, Injector injector) {
        try {
            T instance = targetClass.getDeclaredConstructor().newInstance();
            injectReferences(instance, injector);
            return instance;
        } catch (Exception e) {
            throw new IllegalStateException(
                "Failed to create and inject instance of: " + targetClass.getName(), e
            );
        }
    }
}
