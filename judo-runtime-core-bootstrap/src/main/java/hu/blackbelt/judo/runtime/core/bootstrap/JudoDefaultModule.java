package hu.blackbelt.judo.runtime.core.bootstrap;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.bootstrap.core.DataTypeManagerProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.core.UUIDIdentifierProviderProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.PayloadDaoProcessorProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.RdbmsDapProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.RdbmsSequenceProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.TransformationTraceServiceProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb.HsqldbAtomikosDataSourceProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb.HsqldbServerProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.DefaultActorResolverProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.DefaultMetricsCollectorProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.DefaultVariableResolverProvider;
import hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.ThreadContextProvider;
import hu.blackbelt.judo.runtime.core.dao.core.processors.PayloadDaoProcessor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import hu.blackbelt.mapper.impl.DefaultCoercer;
import net.jmob.guice.conf.core.ConfigurationModule;
import org.hsqldb.server.Server;

import javax.sql.DataSource;
import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.hsqldb.HsqldbServerProvider.*;
import static hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.DefaultMetricsCollectorProvider.*;
import static hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.ThreadContextProvider.THREAD_CONTEXT_DEBUG_THREAD_FORK;
import static hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.ThreadContextProvider.THREAD_CONTEXT_INHERITABLE_CONTEXT;

public class JudoDefaultModule extends AbstractModule {

    public static final String ACCESS_TOKEN_VARIABLE_PROVIDER = "AccessTokenVariableProvider";
    public static final String ACTOR_VARIABLE_PROVIDER = "ActorVariableProvider";
    public static final String ENVIRONMENT_VARIABLE_PROVIDER = "EnvironmentVariableProvider";
    public static final String CURRENT_DATE_PROVIDER = "CurrentDateProvider";
    public static final String CURRENT_TIME_PROVIDER = "CurrentTimeProvider";
    public static final String CURRENT_TIMESTAMP_PROVIDER = "CurrentTimestampProvider";
    public static final String PRINCIPAL_VARIABLE_PROVIDER = "PrincipalVariableProvider";
    public static final String ACTOR_RESOLVER_CHECK_MAPPED_ACTORS = "actorResolverCheckMappedActors";

    private final Object injectModulesTo;
    private final JudoModelSpecification models;

    JudoDefaultModule(Object injectModulesTo, JudoModelSpecification models) {
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
        install(new ConfigurationModule());
        requestInjection(injectModulesTo);

        bind(AsmModel.class).toInstance(models.getAsmModel());

        bind(JudoModuleConfig.class).to(JudoDefaultModuleConfig.class);

        // Model
        bind(JudoModelSpecification.class).toInstance(models);

        // HSQLDB
        bind(Server.class).toProvider(HsqldbServerProvider.class).in(Singleton.class);
        bind(String.class).annotatedWith(Names.named(HSQLDB_SERVER_DATABASE_NAME)).toInstance("judo");
        bind(File.class).annotatedWith(Names.named(HSQLDB_SERVER_DATABASE_PATH)).toInstance(new File(".", "judo.db"));
        bind(Integer.class).annotatedWith(Names.named(HSQLDB_SERVER_PORT)).toInstance(31001);

        // Datasource
        bind(DataSource.class).toProvider(HsqldbAtomikosDataSourceProvider.class).in(Singleton.class);
        bind(Dialect.class).toInstance(Dialect.HSQLDB);

        bind(ExtendableCoercer.class).toInstance(new DefaultCoercer());
        bind(DataTypeManager.class).toProvider(DataTypeManagerProvider.class).in(Singleton.class);
        bind(IdentifierProvider.class).toProvider(UUIDIdentifierProviderProvider.class).in(Singleton.class);

        bind(Sequence.class).toProvider(RdbmsSequenceProvider.class).in(Singleton.class);

        // Context
        bind(Context.class).toProvider(ThreadContextProvider.class).in(Singleton.class);
        bind(Boolean.class).annotatedWith(Names.named(THREAD_CONTEXT_DEBUG_THREAD_FORK)).toInstance(Boolean.FALSE);
        bind(Boolean.class).annotatedWith(Names.named(THREAD_CONTEXT_INHERITABLE_CONTEXT)).toInstance(Boolean.FALSE);

        // Metrics collector
        bind(MetricsCollector.class).toProvider(DefaultMetricsCollectorProvider.class).in(Singleton.class);
        bind(Consumer.class).annotatedWith(Names.named(METRICS_COLLECTOR_CONSUMER)).toInstance((c) -> {});
        bind(Boolean.class).annotatedWith(Names.named(METRICS_COLLECTOR_ENABLED)).toInstance(Boolean.FALSE);
        bind(Boolean.class).annotatedWith(Names.named(METRICS_COLLECTOR_VERBOSE)).toInstance(Boolean.FALSE);


        bind(VariableResolver.class).toProvider(DefaultVariableResolverProvider.class).in(Singleton.class);

        bind(TransformationTraceService.class).toProvider(TransformationTraceServiceProvider.class).in(Singleton.class);

        bind(DAO.class).toProvider(RdbmsDapProvider.class).in(Singleton.class);
        //bind(PayloadDaoProcessor.class).toProvider(PayloadDaoProcessorProvider.class);

        bind(ActorResolver.class).toProvider(DefaultActorResolverProvider.class).in(Singleton.class);
        bind(Boolean.class).annotatedWith(Names.named(ACTOR_RESOLVER_CHECK_MAPPED_ACTORS)).toInstance(Boolean.FALSE);


        /*

//        bind(Long.class).annotatedWith(Names.named(RDBMS_SEQUENCE_START)).toInstance(Long.valueOf(0));
//        bind(Long.class).annotatedWith(Names.named(RDBMS_SEQUENCE_INCREMENT)).toInstance(Long.valueOf(0));
//        bind(Boolean.class).annotatedWith(Names.named(RDBMS_SEQUENCE_CREATE_IF_NOT_EXISTS)).toInstance(Boolean.TRUE);



        bind(Function.class).annotatedWith(Names.named(ACCESS_TOKEN_VARIABLE_PROVIDER)).toProvider(AccessTokenVariableProviderProvider.class);
        bind(Function.class).annotatedWith(Names.named(ACTOR_VARIABLE_PROVIDER)).toProvider(ActorVariableProviderProvider.class);
        bind(Function.class).annotatedWith(Names.named(ENVIRONMENT_VARIABLE_PROVIDER)).toProvider(EnvironmentVariableProviderProvider.class);

        bind(Supplier.class).annotatedWith(Names.named(CURRENT_DATE_PROVIDER)).toProvider(CurrentDateProviderProvider.class);
        bind(Supplier.class).annotatedWith(Names.named(CURRENT_TIME_PROVIDER)).toProvider(CurrentTimeProviderProvider.class);
        bind(Supplier.class).annotatedWith(Names.named(CURRENT_TIMESTAMP_PROVIDER)).toProvider(CurrentTimestampProviderProvider.class);

        bind(Function.class).annotatedWith(Names.named(PRINCIPAL_VARIABLE_PROVIDER)).toProvider(PrincipalVariableProviderProvider.class);

        bind(Dispatcher.class).toProvider(DefaultDispatcherProvider.class);
        bind(Boolean.class).annotatedWith(Names.named(DISPATCHER_METRICS_RETURNED)).toInstance(Boolean.FALSE);
        bind(Boolean.class).annotatedWith(Names.named(DISPATCHER_ENABLE_DEFAULT_VALIDATION)).toInstance(Boolean.FALSE);
        bind(Boolean.class).annotatedWith(Names.named(DISPATCHER_TRIM_STRING)).toInstance(Boolean.FALSE);
        bind(Boolean.class).annotatedWith(Names.named(DISPATCHER_CASE_INSENSITIVE_LIKE)).toInstance(Boolean.FALSE);
        bind(String.class).annotatedWith(Names.named(DISPATCHER_REQUIRED_STRING_VALIDATOR_OPTION)).toInstance("ACCEPT_NON_EMPTY");


        bind(IdentifierSigner.class).toProvider(DefaultIdentifierSignerProvider.class);
        bind(String.class).annotatedWith(Names.named(IDENTIFIER_SIGNER_SECRET)).toInstance(generateNewSecret());


        bind(PasswordPolicy.class).toProvider(NoPasswordPolicyProvider.class);

         */
    }
}
