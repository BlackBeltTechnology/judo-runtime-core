package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.*;

public class DefaultVariableResolverProvider implements Provider<VariableResolver> {

    @Inject
    DataTypeManager dataTypeManager;

    @Inject
    Context context;

    @Inject
    SequenceProvider sequenceProvider;

    @Override
    public VariableResolver get() {
        DefaultVariableResolver variableResolver = new DefaultVariableResolver(dataTypeManager, context);
        variableResolver.registerSupplier("SYSTEM", "current_timestamp", new CurrentTimestampProvider(), false);
        variableResolver.registerSupplier("SYSTEM", "current_date", new CurrentDateProvider(), false);
        variableResolver.registerSupplier("SYSTEM", "current_time", new CurrentTimeProvider(), false);
        variableResolver.registerFunction("ENVIRONMENT", new EnvironmentVariableProvider(), true);
        variableResolver.registerFunction("SEQUENCE", sequenceProvider, false);
        return variableResolver;
    }
}
