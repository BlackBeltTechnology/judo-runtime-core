package hu.blackbelt.judo.runtime.core.bootstrap.core;

import com.google.inject.Provider;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.runtime.core.UUIDIdentifierProvider;

import java.util.UUID;

public class UUIDIdentifierProviderProvider implements Provider<IdentifierProvider<UUID>> {
    @Override
    public IdentifierProvider<UUID> get() {
        return new UUIDIdentifierProvider();
    }
}
