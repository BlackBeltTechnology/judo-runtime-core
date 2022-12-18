package hu.blackbelt.judo.runtime.core.dispatcher;

import java.util.function.Function;
import java.util.function.Supplier;

public interface VariableResolverManager {

    void registerSupplier(final String category, final String key, @SuppressWarnings("rawtypes") final Supplier supplier, final boolean cacheable);

    @SuppressWarnings("unchecked")
    void registerFunction(final String category, @SuppressWarnings("rawtypes") final Function function, final boolean cacheable);

    void unregisterSupplier(final String category, final String key);

    void unregisterFunction(final String category);

}
