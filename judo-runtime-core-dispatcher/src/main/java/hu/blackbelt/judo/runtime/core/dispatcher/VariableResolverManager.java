package hu.blackbelt.judo.runtime.core.dispatcher;

import java.util.function.Function;
import java.util.function.Supplier;

public interface VariableResolverManager {

    void registerSupplier(final String category, final String key, final Supplier supplier, final boolean cacheable);

    void registerFunction(final String category, final Function function, final boolean cacheable);

    void unregisterSupplier(final String category, final String key);

    void unregisterFunction(final String category);

}
