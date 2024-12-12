package hu.blackbelt.judo.runtime.core.guice.core;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import hu.blackbelt.mapper.impl.DefaultCoercer;

public class CoercererProvider implements Provider<Coercer> {
    @Inject
    ExtendableCoercer extendableCoercer;

    @Override
    public Coercer get() {
        return extendableCoercer;
    }
}
