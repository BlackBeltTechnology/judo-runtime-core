package hu.blackbelt.judo.services.security;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import java.util.Map;
import java.util.Optional;

@Component(immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class NoPasswordPolicy<ID> implements PasswordPolicy<ID> {

    @Override
    public Optional<ID> apply(Map<String, Object> stringObjectMap) {
        return Optional.empty();
    }
}
