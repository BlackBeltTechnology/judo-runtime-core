package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.DefaultVariableResolver;

public class DefaultVariableResolverProvider implements Provider<VariableResolver> {

    DataTypeManager dataTypeManager;
    Context context;

    @Inject
    public DefaultVariableResolverProvider(DataTypeManager dataTypeManager, Context context) {
        this.dataTypeManager = dataTypeManager;
        this.context = context;
    }

    @Override
    public VariableResolver get() {
        return new DefaultVariableResolver(dataTypeManager, context);
    }
}
