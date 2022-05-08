package hu.blackbelt.judo.runtime.core.bootstrap.core;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.mapper.api.ExtendableCoercer;

public class DataTypeManagerProvider implements Provider<DataTypeManager> {

    private final ExtendableCoercer coercer;

    @Inject
    public DataTypeManagerProvider(ExtendableCoercer coercer) {
        this.coercer = coercer;
    }

    @Override
    public DataTypeManager get() {
        return new DataTypeManager(coercer);
    }
}
