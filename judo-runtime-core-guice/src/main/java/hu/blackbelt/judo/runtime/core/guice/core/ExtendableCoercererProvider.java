package hu.blackbelt.judo.runtime.core.guice.core;

import com.google.inject.Provider;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import hu.blackbelt.mapper.impl.DefaultCoercer;

public class ExtendableCoercererProvider implements Provider<ExtendableCoercer> {
    @Override
    public ExtendableCoercer get() {
        return new DefaultCoercer();
    }
}
