package hu.blackbelt.judo.runtime.core.dispatcher.environment;

import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

@NoArgsConstructor
@RequiredArgsConstructor
@Slf4j
public class DefaultVariableResolver implements VariableResolver {

    @NonNull
    DataTypeManager dataTypeManager;

    @NonNull
    Context context;

    private Map<String, Supplier<Object>> suppliers = new ConcurrentHashMap<>();
    private Map<String, Function<String, Object>> functions = new ConcurrentHashMap<>();

    private static final String JUDO_ENVIRONMENT_VARIABLE_PROVIDER = "judo.environment-variable-provider";
    private static final String CATEGORY = "category";
    private static final String KEY = "key";
    private static final String CACHEABLE = "cacheable";

//    private FunctionTracker supplierTracker;
//    private FunctionTracker functionTracker;
    private Collection<String> cacheableKeys = new HashSet<>();

    /*
    @Activate
    @SneakyThrows
    void start(final BundleContext context) {
        cacheableKeys.clear();
        supplierTracker = new FunctionTracker<Supplier>(context, Supplier.class, suppliers) {
            @Override
            void register(final String category, final String key, final boolean cacheable, final Supplier supplier) {
                registerSupplier(category, key, supplier, cacheable);
            }

            @Override
            void unregister(final String category, final String key) {
                unregisterSupplier(category, key);
            }
        };
        functionTracker = new FunctionTracker<Function>(context, Function.class, functions) {
            @Override
            void register(final String category, final String key, final boolean cacheable, final Function function) {
                registerFunction(category, function, cacheable);
            }

            @Override
            void unregister(final String category, final String key) {
                unregisterFunction(category);
            }
        };
        supplierTracker.open(true);
        functionTracker.open(true);
    }

    @Deactivate
    void stop() {
        if (supplierTracker != null) {
            supplierTracker.close();
        }
        if (functionTracker != null) {
            functionTracker.close();
        }
    }
*/
    @Override
    public <T> T resolve(final Class<T> type, final String category, final String key) {
        final Map<String, Object> variables = context.putIfAbsent(Dispatcher.VARIABLES_KEY, new HashMap<>());
        final Object value;
        if (!variables.containsKey(category + ":" + key)) {
            final boolean cacheable;
            if (suppliers.containsKey(category + ":" + key)) {
                value = suppliers.get(category + ":" + key).get();
                cacheable = cacheableKeys.contains(category + ":" + key);
            } else if (functions.containsKey(category)) {
                value = functions.get(category).apply(key);
                cacheable = cacheableKeys.contains(category + ":*");
            } else {
                log.warn("Undefined variable: {}.{}", category, key);
                value = null;
                cacheable = false;
            }

            if (cacheable) {
                variables.put(category + ":" + key, value);
            }
        } else {
            value = variables.get(category + ":" + key);
        }

        return dataTypeManager.getCoercer().coerce(value, type);
    }

    public void registerSupplier(final String category, final String key, final Supplier supplier, final boolean cacheable) {
        suppliers.put(category + ":" + key, supplier);
        if (cacheable) {
            cacheableKeys.add(category + ":" + (key != null ? key : "*"));
        }
    }

    public void registerFunction(final String category, final Function function, final boolean cacheable) {
        functions.put(category, function);
        if (cacheable) {
            cacheableKeys.add(category + ":*");
        }
    }

    public void unregisterSupplier(final String category, final String key) {
        suppliers.remove(category + ":" + key);
    }

    public void unregisterFunction(final String category) {
        functions.remove(category);
    }

    /*
    private abstract class FunctionTracker<T> extends ServiceTracker<T, T> {
        private final Map<String, ? extends T> functions;

        FunctionTracker(final BundleContext bundleContext, final Class<T> clazz, final Map<String, ? extends T> functions) throws InvalidSyntaxException {
            super(bundleContext, bundleContext.createFilter("(&(" + Constants.OBJECTCLASS + "=" + clazz.getName() + ")(" + JUDO_ENVIRONMENT_VARIABLE_PROVIDER + "=true))"), null);
            this.functions = functions;
        }

        @Override
        public T addingService(final ServiceReference<T> reference) {
            final T function = super.addingService(reference);
            add(reference, function);
            return function;
        }

        @Override
        public void modifiedService(final ServiceReference<T> reference, final T service) {
            String category = null;
            String key = null;

            for (Map.Entry<String, ? extends T> f : functions.entrySet()) {
                if (Objects.equals(f.getValue(), service)) {
                    String[] parts = f.getKey().split(":");
                    if (parts.length > 0) {
                        category = parts[0];
                    }
                    if (parts.length > 1) {
                        key = parts[1];
                    }
                }
            }

            if (category != null) {
                unregister(category, key);
            }

            add(reference, service);
            super.modifiedService(reference, service);
        }

        private void add(final ServiceReference<T> reference, final T function) {
            final Object category = reference.getProperty(CATEGORY);
            final Object key = reference.getProperty(KEY);
            final Object cacheable = reference.getProperty(CACHEABLE);

            if (category instanceof String && (key == null || key instanceof String)) {
                register((String) category, (String) key, cacheable == null || Boolean.parseBoolean(cacheable.toString()), function);
            } else {
                log.warn("Missing (or invalid) {} properties");
            }
        }

        abstract void register(String category, String key, boolean cacheable, T function);

        abstract void unregister(String category, String key);

        @Override
        public void removedService(final ServiceReference<T> reference, final T service) {
            final Object category = reference.getProperty(CATEGORY);
            final Object key = reference.getProperty(KEY);

            if (category instanceof String && (key == null || key instanceof String)) {
                unregister((String) category, (String) key);
            } else {
                log.warn("Missing (or invalid) {} property", CATEGORY, KEY);
            }

            super.removedService(reference, service);
        }
    }
*/
}
