package hu.blackbelt.judo.services.security;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public interface PasswordPolicy<ID> extends Function<Map<String, Object>, Optional<ID>> {
}
