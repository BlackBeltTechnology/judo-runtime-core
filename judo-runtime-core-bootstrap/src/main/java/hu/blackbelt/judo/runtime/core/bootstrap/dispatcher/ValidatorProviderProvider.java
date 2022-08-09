package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.runtime.core.validator.DefaultValidatorProvider;
import hu.blackbelt.judo.runtime.core.validator.ValidatorProvider;

public class ValidatorProviderProvider implements Provider<ValidatorProvider> {

    @Inject
    DAO dao;

    @Inject
    IdentifierProvider identifierProvider;

    @Inject
    Context context;

    @Override
    public ValidatorProvider get() {
        return new DefaultValidatorProvider(dao, identifierProvider, context);
    }
}
