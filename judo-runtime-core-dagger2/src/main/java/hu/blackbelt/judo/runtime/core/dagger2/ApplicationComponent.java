package hu.blackbelt.judo.runtime.core.dagger2;

import dagger.Component;
import hu.blackbelt.judo.runtime.core.dagger2.*;
import hu.blackbelt.judo.runtime.core.dagger2.accessmanager.DefaultAccessManagerModule;
import hu.blackbelt.judo.runtime.core.dagger2.dao.rdbms.*;
import hu.blackbelt.judo.runtime.core.dagger2.database.Database;
import hu.blackbelt.judo.runtime.core.dagger2.dispatcher.*;
import hu.blackbelt.judo.runtime.core.dagger2.security.NoPasswordPolicyModule;
import hu.blackbelt.judo.runtime.core.dagger2.security.OpenIdConfigurationProviderModule;
import hu.blackbelt.judo.runtime.core.dagger2.security.PathInfoRealmExtractorModule;
import hu.blackbelt.judo.runtime.core.dagger2.security.TokenModule;

@JudoApplicationScope
@Component(modules = {
        JudoDefaultConfiguration.class,
        DefaultAccessManagerModule.class,
        ModifyStatementExecutorModule.class,
        PlatformTransactionManagerModule.class,
        QueryFactoryModule.class,
        RdbmsBuilderModule.class,
        RdbmsDAOModule.class,
        RdbmsInstanceCollectorModule.class,
        RdbmsResolverModule.class,
        SelectStatementExecutorModule.class,
        TransformationTraceServiceModule.class,
        DefaultActorResolverModule.class,
        DefaultDispatcherModule.class,
        DefaultIdentifierSignerModule.class,
        DefaultMetricsCollectorModule.class,
        DefaultPayloadValidatorModule.class,
        DefaultVariableResolverModule.class,
        DispatcherFunctionProviderModule.class,
        OperationCallInterceptorProviderModule.class,
        ValidatorProviderModule.class,
        // ModelsModule.class,
        NoPasswordPolicyModule.class,
        PathInfoRealmExtractorModule.class,
        TokenModule.class,
        OpenIdConfigurationProviderModule.class
        // DataSourceModule.class
}, dependencies = {
        Utils.class,
        ModelHolder.class,
        Database.class
})
public interface ApplicationComponent extends JudoDefaultComponent {
    void inject(AbstractApplication abstractApplication);
}
