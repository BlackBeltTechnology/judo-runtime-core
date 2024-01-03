package hu.blackbelt.judo.runtime.core.dagger2;

import dagger.Component;
import hu.blackbelt.judo.runtime.core.dagger2.core.CoercerModule;
import hu.blackbelt.judo.runtime.core.dagger2.core.DataTypeManagerModule;
import hu.blackbelt.judo.runtime.core.dagger2.core.UUIDIdentifierProviderModule;
import hu.blackbelt.judo.runtime.core.dagger2.dao.rdbms.*;
import hu.blackbelt.judo.runtime.core.dagger2.dispatcher.*;

@JudoApplicationScope
@Component(modules = {
        UtilsConfiguration.class,
        DataTypeManagerModule.class,
        UUIDIdentifierProviderModule.class,
        CoercerModule.class,
        SimpleLiquibaseExecutorModule.class,
        ThreadContextModule.class
})

public interface UtilsComponent extends Utils {
}
