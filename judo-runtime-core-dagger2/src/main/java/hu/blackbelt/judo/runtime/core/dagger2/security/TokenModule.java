package hu.blackbelt.judo.runtime.core.dagger2.security;

import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.osgi.filestore.security.api.TokenIssuer;
import hu.blackbelt.osgi.filestore.security.api.TokenValidator;

import javax.annotation.Nullable;

@Module
public class TokenModule {

    @JudoApplicationScope
    @Provides
    @Nullable
    TokenIssuer providesTokenIssuer() {
        return null;
    }

    @JudoApplicationScope
    @Provides
    @Nullable
    TokenValidator providesTokenValidator() {
        return null;
    }

}
