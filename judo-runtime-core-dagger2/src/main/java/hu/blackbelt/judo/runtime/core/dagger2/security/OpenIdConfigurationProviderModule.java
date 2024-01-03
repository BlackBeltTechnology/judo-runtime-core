package hu.blackbelt.judo.runtime.core.dagger2.security;

import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.security.OpenIdConfigurationProvider;

import javax.annotation.Nullable;

@Module
public class OpenIdConfigurationProviderModule {

    @JudoApplicationScope
    @Provides
    @Nullable
    OpenIdConfigurationProvider providesOpenIdConfigurationProvider() {
        return null;
    }
}
