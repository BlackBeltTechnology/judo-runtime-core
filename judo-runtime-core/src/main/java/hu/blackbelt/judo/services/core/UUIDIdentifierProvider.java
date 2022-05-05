package hu.blackbelt.judo.services.core;

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import java.util.UUID;

@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class UUIDIdentifierProvider implements IdentifierProvider<UUID> {

    @Override
    public UUID get() {
        return UUID.randomUUID();
    }

    @Override
    public Class<UUID> getType() {
        return UUID.class;
    }

    @Override
    public String getName() {
        return "__identifier";
    }
}
