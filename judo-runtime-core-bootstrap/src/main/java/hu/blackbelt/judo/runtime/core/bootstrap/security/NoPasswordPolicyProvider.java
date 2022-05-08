package hu.blackbelt.judo.runtime.core.bootstrap.security;

import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.security.NoPasswordPolicy;
import hu.blackbelt.judo.runtime.core.security.PasswordPolicy;

public class NoPasswordPolicyProvider implements Provider<PasswordPolicy> {

    @Override
    public PasswordPolicy get() {
        return new NoPasswordPolicy();
    }
}
