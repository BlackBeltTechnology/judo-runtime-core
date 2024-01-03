package hu.blackbelt.judo.runtime.core.dagger2.core;

import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import hu.blackbelt.mapper.impl.DefaultCoercer;

@Module
public class CoercerModule {

    @JudoApplicationScope
    @Provides
    public ExtendableCoercer provideExtendableCoercer() {
        return new DefaultCoercer();
    }

    @JudoApplicationScope
    @Provides
    public Coercer providesCoercer(ExtendableCoercer coercer) {
        return coercer;
    }


}
