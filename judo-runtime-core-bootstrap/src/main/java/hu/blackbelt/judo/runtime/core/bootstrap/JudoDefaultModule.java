package hu.blackbelt.judo.runtime.core.bootstrap;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AccessManager;
import hu.blackbelt.judo.runtime.core.bootstrap.accessmanager.DefaultAccessManagerProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.core.DataTypeManagerProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.core.UUIDIdentifierProviderProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.*;
import hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.*;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.ModifyStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.SelectStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dispatcher.DispatcherFunctionProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import hu.blackbelt.mapper.impl.DefaultCoercer;

import javax.transaction.TransactionManager;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.function.Consumer;

import static hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.RdbmsDAOProvider.RDBMS_DAO_MARK_SELECTED_RANGE_ITEMS;
import static hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.RdbmsDAOProvider.RDBMS_DAO_OPTIMISTIC_LOCK_ENABLED;
import static hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.DefaultDispatcherProvider.*;
import static hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.DefaultIdentifierSignerProvider.IDENTIFIER_SIGNER_SECRET;
import static hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.DefaultMetricsCollectorProvider.*;
import static hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.ThreadContextProvider.THREAD_CONTEXT_DEBUG_THREAD_FORK;
import static hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.ThreadContextProvider.THREAD_CONTEXT_INHERITABLE_CONTEXT;

public class JudoDefaultModule extends AbstractModule {

    public static final String ACTOR_RESOLVER_CHECK_MAPPED_ACTORS = "actorResolverCheckMappedActors";

    private final Object injectModulesTo;
    private final JudoModelHolder models;

    public JudoDefaultModule(Object injectModulesTo, JudoModelHolder models) {
        this.injectModulesTo = injectModulesTo;
        this.models = models;
    }

    public String generateNewSecret() {
        final SecureRandom random;
        try {
            random = SecureRandom.getInstanceStrong();
            final byte[] values = new byte[1024 / 8];
            random.nextBytes(values);
            return Base64.getEncoder().encodeToString(values);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
    protected void configure() {
        requestInjection(injectModulesTo);

        bind(AsmModel.class).toInstance(models.getAsmModel());

        // Model
        bind(JudoModelHolder.class).toInstance(models);
        
        bind(RdbmsResolver.class).toProvider(RdbmsResolverProvider.class).in(Singleton.class);
        bind(VariableResolver.class).toProvider(DefaultVariableResolverProvider.class).in(Singleton.class);
        bind(RdbmsBuilder.class).toProvider(RdbmsBuilderProvider.class).in(Singleton.class);
        bind(QueryFactory.class).toProvider(QueryFactoryProvider.class).in(Singleton.class);
        bind(SelectStatementExecutor.class).toProvider(SelectStatementExecutorProvider.class).in(Singleton.class);
        bind(ModifyStatementExecutor.class).toProvider(ModifyStatementExecutorProvider.class).in(Singleton.class);

        bind(ExtendableCoercer.class).toInstance(new DefaultCoercer());
        bind(DataTypeManager.class).toProvider(DataTypeManagerProvider.class).in(Singleton.class);
        bind(IdentifierProvider.class).toProvider(UUIDIdentifierProviderProvider.class).in(Singleton.class);
        
        bind(IdentifierSigner.class).toProvider(DefaultIdentifierSignerProvider.class).in(Singleton.class);
        bind(String.class).annotatedWith(Names.named(IDENTIFIER_SIGNER_SECRET)).toInstance(generateNewSecret());

        // Access manager
        bind(AccessManager.class).toProvider(DefaultAccessManagerProvider.class);

        // Context
        bind(Context.class).toProvider(ThreadContextProvider.class).in(Singleton.class);
        bind(Boolean.class).annotatedWith(Names.named(THREAD_CONTEXT_DEBUG_THREAD_FORK)).toInstance(Boolean.FALSE);
        bind(Boolean.class).annotatedWith(Names.named(THREAD_CONTEXT_INHERITABLE_CONTEXT)).toInstance(Boolean.FALSE);

        // Metrics collector
        bind(MetricsCollector.class).toProvider(DefaultMetricsCollectorProvider.class).in(Singleton.class);
        bind(Consumer.class).annotatedWith(Names.named(METRICS_COLLECTOR_CONSUMER)).toInstance((c) -> {});
        bind(Boolean.class).annotatedWith(Names.named(METRICS_COLLECTOR_ENABLED)).toInstance(Boolean.FALSE);
        bind(Boolean.class).annotatedWith(Names.named(METRICS_COLLECTOR_VERBOSE)).toInstance(Boolean.FALSE);

        bind(TransformationTraceService.class).toProvider(TransformationTraceServiceProvider.class).in(Singleton.class);

        bind(InstanceCollector.class).toProvider(RdbmsInstanceCollectorProvider.class).in(Singleton.class);
        bind(DAO.class).toProvider(RdbmsDAOProvider.class).in(Singleton.class);
        bind(Boolean.class).annotatedWith(Names.named(RDBMS_DAO_OPTIMISTIC_LOCK_ENABLED)).toInstance(true);
        bind(Integer.class).annotatedWith(Names.named(RDBMS_DAO_MARK_SELECTED_RANGE_ITEMS)).toInstance(1000);
        bind(Boolean.class).annotatedWith(Names.named(RDBMS_DAO_MARK_SELECTED_RANGE_ITEMS)).toInstance(false);

        bind(ActorResolver.class).toProvider(DefaultActorResolverProvider.class).in(Singleton.class);
        bind(Boolean.class).annotatedWith(Names.named(ACTOR_RESOLVER_CHECK_MAPPED_ACTORS)).toInstance(Boolean.FALSE);


        // Dispatcher
        bind(DispatcherFunctionProvider.class).toProvider(DispatcherFunctionProviderProvider.class).in(Singleton.class);

        bind(Dispatcher.class).toProvider(DefaultDispatcherProvider.class).in(Singleton.class);
        bind(Boolean.class).annotatedWith(Names.named(DISPATCHER_METRICS_RETURNED)).toInstance(Boolean.FALSE);
        bind(Boolean.class).annotatedWith(Names.named(DISPATCHER_ENABLE_DEFAULT_VALIDATION)).toInstance(Boolean.FALSE);
        bind(Boolean.class).annotatedWith(Names.named(DISPATCHER_TRIM_STRING)).toInstance(Boolean.FALSE);
        bind(Boolean.class).annotatedWith(Names.named(DISPATCHER_CASE_INSENSITIVE_LIKE)).toInstance(Boolean.FALSE);
        bind(String.class).annotatedWith(Names.named(DISPATCHER_REQUIRED_STRING_VALIDATOR_OPTION)).toInstance("ACCEPT_NON_EMPTY");

    }
}
