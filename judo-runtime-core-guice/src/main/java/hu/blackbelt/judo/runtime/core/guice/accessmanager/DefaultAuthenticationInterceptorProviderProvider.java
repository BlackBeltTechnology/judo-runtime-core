package hu.blackbelt.judo.runtime.core.guice.accessmanager;

import com.google.inject.Provider;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AuthenticationInterceptor;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AuthenticationInterceptorProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DefaultAuthenticationInterceptorProviderProvider implements Provider<AuthenticationInterceptorProvider> {
    @Override
    public AuthenticationInterceptorProvider get() {
        final List<AuthenticationInterceptor> interceptors = new ArrayList<>();

        return new AuthenticationInterceptorProvider() {
            @Override
            public Collection<AuthenticationInterceptor> getAuthenticationInterceptors() {
                return interceptors;
            }
        };
    }
}
