package hu.blackbelt.judo.runtime.core.dispatcher.environment;

import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import lombok.*;
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
    @Setter
    DataTypeManager dataTypeManager;

    @NonNull
    @Setter
    Context context;

    private Map<String, Supplier<Object>> suppliers = new ConcurrentHashMap<>();
    private Map<String, Function<String, Object>> functions = new ConcurrentHashMap<>();

    private Collection<String> cacheableKeys = new HashSet<>();

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

    @SuppressWarnings("unchecked")
	public void registerSupplier(final String category, final String key, @SuppressWarnings("rawtypes") final Supplier supplier, final boolean cacheable) {
        suppliers.put(category + ":" + key, supplier);
        if (cacheable) {
            cacheableKeys.add(category + ":" + (key != null ? key : "*"));
        }
    }

    @SuppressWarnings("unchecked")
	public void registerFunction(final String category, @SuppressWarnings("rawtypes") final Function function, final boolean cacheable) {
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

}
