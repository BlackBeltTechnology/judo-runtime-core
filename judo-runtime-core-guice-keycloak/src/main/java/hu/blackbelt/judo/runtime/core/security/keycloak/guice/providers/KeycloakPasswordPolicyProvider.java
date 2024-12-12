package hu.blackbelt.judo.runtime.core.security.keycloak.guice.providers;

import com.google.inject.Inject;
import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.security.NoPasswordPolicy;
import hu.blackbelt.judo.runtime.core.security.PasswordPolicy;
import hu.blackbelt.judo.runtime.core.security.keycloak.SameEmailPasswordPolicy;
import hu.blackbelt.judo.runtime.core.security.keycloak.SameUsernamePasswordPolicy;
import hu.blackbelt.judo.runtime.core.security.keycloak.guice.KeycloakConfigurationQualifiers;

import javax.annotation.Nullable;

public class KeycloakPasswordPolicyProvider implements Provider<PasswordPolicy> {
    @Inject(optional = true)
    @KeycloakConfigurationQualifiers.KeycloakSecurityPasswordPolicyType
    @Nullable
    String securityPasswordPolicyType;

    @Override
    public PasswordPolicy get() {
        if ("SAME_USERNAME".equals(securityPasswordPolicyType)) {
            return new SameUsernamePasswordPolicy();
        } else if ("SAME_EMAIL".equals(securityPasswordPolicyType)) {
            return new SameEmailPasswordPolicy();
        }
        return new NoPasswordPolicy();
    }
}
