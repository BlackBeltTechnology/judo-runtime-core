package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.validator.DefaultValidatorProvider;
import hu.blackbelt.judo.runtime.core.validator.ValidatorProvider;

public class ValidatorProviderProvider implements Provider<ValidatorProvider> {
    @Override
    public ValidatorProvider get() {
        return new DefaultValidatorProvider();
    }
}
