package hu.blackbelt.judo.services.dispatcher.context;

import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.services.core.DataTypeManager;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.*;

import java.util.Map;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkArgument;

@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
@NoArgsConstructor
@RequiredArgsConstructor
@Slf4j
public class ThreadContext implements Context {

    public static final String DEBUG_THREAD_FORK = "debugThreadFork";
    public static final String INHERITABLE_CONTEXT = "inheritableContext";

    private static ThreadLocal<Map<String, Object>> THREADLOCAL = new InheritableThreadLocal<>();
    private boolean debugThreadFork = false;
    private boolean inheritableContext = true;

    @Activate
    public void activate(ComponentContext context) {
        if (context.getProperties().get(DEBUG_THREAD_FORK) != null) {
            debugThreadFork = Boolean.parseBoolean(context.getProperties().get(DEBUG_THREAD_FORK).toString());
        }
        if (context.getProperties().get(INHERITABLE_CONTEXT) != null) {
            inheritableContext = Boolean.parseBoolean(context.getProperties().get(INHERITABLE_CONTEXT).toString());
        }
        THREADLOCAL.remove();
        if (inheritableContext) {
            THREADLOCAL = new InheritableThreadLocal<>() {
                @Override
                protected Map<String, Object> childValue(Map<String, Object> parentValue) {
                    if (debugThreadFork) {
                        try {
                            throw new IllegalStateException("=== fork === | " + Thread.currentThread().getName());
                        } catch (IllegalStateException e) {
                            log.debug("!!!! FORK !!!!", e);
                        }
                    }
                    return super.childValue(parentValue);
                }
            };
        } else {
            THREADLOCAL = new ThreadLocal<>();
        }
    }

    @NonNull
    @Reference(policyOption = ReferencePolicyOption.GREEDY)
    DataTypeManager dataTypeManager;

    @Override
    public Object get(final String key) {
        final Map<String, Object> perThreadResources = THREADLOCAL.get();
        if (perThreadResources != null) {
            synchronized (perThreadResources) {
                return perThreadResources.get(key);
            }
        } else {
            return null;
        }
    }

    @Override
    public <T> T getAs(final Class<T> clazz, final String key) {
        return dataTypeManager.getCoercer().coerce(get(key), clazz);
    }

    @Override
    public void put(final String key, final Object value) {
        checkArgument(key != null, "Key must not be null");

        if (value == null) {
            remove(key);
        } else {
            ensureResourcesInitialized().put(key, value);
        }
    }

    @Override
    public <T> T putIfAbsent(String key, T value) {
        checkArgument(key != null, "Key must not be null");

        // TODO: It's not compatible with standard Map implementation
        // return (T) ensureResourcesInitialized().putIfAbsent(key, value);

        final Map<String, Object> perThreadResources = ensureResourcesInitialized();
        synchronized (perThreadResources) {
            perThreadResources.putIfAbsent(key, value);
        }
        return (T) get(key);
    }

    @Override
    public Object remove(final String key) {
        final Map<String, Object> perThreadResources = THREADLOCAL.get();
        if (perThreadResources != null) {
            synchronized (perThreadResources) {
                return perThreadResources.remove(key);
            }
        } else {
            return null;
        }
    }

    @Override
    public void removeAll() {
        final Map<String, Object> perThreadResources = THREADLOCAL.get();
        if (perThreadResources != null) {
            synchronized (perThreadResources) {
                perThreadResources.clear();
            }
        }
        THREADLOCAL.remove();
    }

    private Map<String, Object> ensureResourcesInitialized() {
        if (THREADLOCAL.get() == null) {
            THREADLOCAL.set(new TreeMap<>());
        }
        return THREADLOCAL.get();
    }
}
