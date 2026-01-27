package hu.blackbelt.judo.runtime.core.guice.testkit.util;

import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptor;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A mutable implementation of {@link OperationCallInterceptorProvider} for test scenarios.
 *
 * <p>This provider allows adding, removing, and clearing interceptors during test setup,
 * enabling integration testing of interceptors within the dispatcher flow.
 *
 * <p>Usage:
 * <pre>
 * {@code
 * TestOperationCallInterceptorProvider provider = new TestOperationCallInterceptorProvider();
 * provider.addInterceptor(myInterceptor);
 *
 * // Pass to JudoDefaultModule builder
 * JudoDefaultModule.builder()
 *     .operationCallInterceptorProvider(provider)
 *     .build();
 * }
 * </pre>
 *
 * <p>For most test scenarios, use {@link hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture#addInterceptor(Class)}
 * or {@link hu.blackbelt.judo.runtime.core.guice.testkit.fixture.JudoRuntimeFixture#addInterceptor(OperationCallInterceptor)}
 * instead of using this class directly.
 *
 * @see OperationCallInterceptorProvider
 * @see OperationCallInterceptor
 */
public class TestOperationCallInterceptorProvider implements OperationCallInterceptorProvider {

    private final List<OperationCallInterceptor> interceptors = new ArrayList<>();

    /**
     * Add an interceptor to be invoked during operation calls.
     * Interceptors are invoked in the order they are added.
     *
     * @param interceptor The interceptor to add
     * @throws IllegalArgumentException if interceptor is null
     */
    public void addInterceptor(OperationCallInterceptor interceptor) {
        if (interceptor == null) {
            throw new IllegalArgumentException("Interceptor cannot be null");
        }
        interceptors.add(interceptor);
    }

    /**
     * Remove a specific interceptor.
     *
     * @param interceptor The interceptor to remove
     * @return true if the interceptor was found and removed, false otherwise
     */
    public boolean removeInterceptor(OperationCallInterceptor interceptor) {
        return interceptors.remove(interceptor);
    }

    /**
     * Remove all registered interceptors.
     */
    public void clearInterceptors() {
        interceptors.clear();
    }

    /**
     * Get all registered interceptors.
     * Returns a copy of the internal list to prevent external modification.
     *
     * @return A new list containing all registered interceptors
     */
    public List<OperationCallInterceptor> getInterceptors() {
        return new ArrayList<>(interceptors);
    }

    /**
     * Get the number of registered interceptors.
     *
     * @return The count of registered interceptors
     */
    public int size() {
        return interceptors.size();
    }

    /**
     * Check if any interceptors are registered.
     *
     * @return true if no interceptors are registered, false otherwise
     */
    public boolean isEmpty() {
        return interceptors.isEmpty();
    }

    @Override
    public Collection<OperationCallInterceptor> getCallOperationInterceptors() {
        return new ArrayList<>(interceptors);
    }
}
