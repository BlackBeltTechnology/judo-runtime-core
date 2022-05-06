package hu.blackbelt.judo.services.security;

import java.util.Map;
import java.util.Optional;

public class NoPasswordPolicy<ID> implements PasswordPolicy<ID> {

    @Override
    public Optional<ID> apply(Map<String, Object> stringObjectMap) {
        return Optional.empty();
    }
}
