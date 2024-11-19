package hu.blackbelt.judo.runtime.core.accessmanager.api;

import java.util.ArrayList;
import java.util.Collection;

public interface AuthenticationInterceptorProvider {
    default Collection<AuthenticationInterceptor> getAuthenticationInterceptors() {
        return new ArrayList<>();
    };
}
