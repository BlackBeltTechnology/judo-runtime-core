package hu.blackbelt.judo.runtime.core.dagger2;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

import com.google.inject.Singleton;
import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dao.api.PayloadValidator;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.Sequence;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.builder.jql.JqlExpressionBuilderConfig;
import hu.blackbelt.judo.meta.expression.builder.jql.asm.AsmJqlExtractor;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.UUIDIdentifierProvider;
import hu.blackbelt.judo.runtime.core.accessmanager.DefaultAccessManager;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AccessManager;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.ModifyStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.SelectStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.liquibase.SimpleLiquibaseExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.AncestorNameFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.DescendantNameFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;
import hu.blackbelt.judo.runtime.core.dispatcher.*;
import hu.blackbelt.judo.runtime.core.dispatcher.context.ThreadContext;
import hu.blackbelt.judo.runtime.core.dispatcher.environment.*;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;
import hu.blackbelt.judo.runtime.core.query.CustomJoinDefinition;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.runtime.core.security.*;
import hu.blackbelt.judo.runtime.core.validator.DefaultPayloadValidator;
import hu.blackbelt.judo.runtime.core.validator.DefaultValidatorProvider;
import hu.blackbelt.judo.runtime.core.validator.ValidatorProvider;
import hu.blackbelt.judo.tatami.asm2rdbms.Asm2RdbmsTransformationTrace;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.judo.tatami.core.TransformationTraceServiceImpl;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import hu.blackbelt.osgi.filestore.security.api.TokenIssuer;
import hu.blackbelt.osgi.filestore.security.api.TokenValidator;
import org.eclipse.emf.common.util.*;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.annotation.Nullable;
import javax.inject.Named;
import javax.sql.DataSource;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNullElse;


@Module
public class JudoAggregatedModule {

    public static final String ACTOR_RESOLVER_CHECK_MAPPED_ACTORS = "actorResolverCheckMappedActors";
    public static final String QUERY_FACTORY_CUSTOM_JOIN_DEFINITIONS = "queryFactoryCustomJoinDefinitions";
    public static final String RDBMS_DAO_OPTIMISTIC_LOCK_ENABLED = "rdbmsDaoOptimisticLockEnabled";
    public static final String RDBMS_DAO_MARK_SELECTED_RANGE_ITEMS = "rdbmsDaoMarkSelectedRangeItems";
    public static final String RDBMS_SELECT_CHUNK_SIZE = "rdbmsDaoChunkSize";
    public static final String DISPATCHER_METRICS_RETURNED = "dispatcherMetricsReturned";
    public static final String DISPATCHER_ENABLE_VALIDATION = "dispatcherEnableDefaultValidation";
    public static final String DISPATCHER_TRIM_STRING = "dispatcherTrimString";
    public static final String DISPATCHER_CASE_INSENSITIVE_LIKE = "dispatcherCaseInsensitiveLike";
    public static final String IDENTIFIER_SIGNER_SECRET = "identifierSignerSecret";
    public static final String METRICS_COLLECTOR_CONSUMER = "metricsCollectorConsumer";
    public static final String METRICS_COLLECTOR_ENABLED = "metricsCollectorEnabled";
    public static final String METRICS_COLLECTOR_VERBOSE = "metricsCollectorVerbose";
    public static final String PAYLOAD_VALIDATOR_REQUIRED_STRING_VALIDATOR_OPTION = "payloadValidatorRequiredStringValidatorOption";
    public static final String THREAD_CONTEXT_DEBUG_THREAD_FORK = "threadContextDebugThreadFork";
    public static final String THREAD_CONTEXT_INHERITABLE_CONTEXT = "threadContextInheritableContext";

    /*
    private final Object injectModulesTo;
    private final JudoModelLoader judoModelLoader;
    private Boolean bindModelHolder;

    public static class JudoDefaultModuleBuilder {
        private Object injectModulesTo = false;
        private JudoModelLoader judoModelLoader = null;
        private Boolean bindModelHolder = true;
    }

    public JudoDefaultModule(Object injectModulesTo, JudoModelLoader models) {
        this.injectModulesTo = injectModulesTo;
        this.judoModelLoader = models;
        this.bindModelHolder = true;
    }

    @Builder
    public JudoDefaultModule(Object injectModulesTo, JudoModelLoader judoModelLoader, Boolean bindModelHolder) {
        this.injectModulesTo = injectModulesTo;
        this.judoModelLoader = judoModelLoader;
        this.bindModelHolder = bindModelHolder;
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
        if (injectModulesTo != null) {
            requestInjection(injectModulesTo);
        }

        bind(AsmModel.class).toInstance(judoModelLoader.getAsmModel());
        bind(RdbmsModel.class).toInstance(judoModelLoader.getRdbmsModel());
        bind(MeasureModel.class).toInstance(judoModelLoader.getMeasureModel());
        bind(LiquibaseModel.class).toInstance(judoModelLoader.getLiquibaseModel());
        bind(ExpressionModel.class).toInstance(judoModelLoader.getExpressionModel());

        // Model
        if (bindModelHolder) {
            bind(JudoModelLoader.class).toInstance(judoModelLoader);
        }

        bind(RdbmsResolver.class).toProvider(RdbmsResolverProvider.class).in(Singleton.class);
        bind(VariableResolver.class).toProvider(DefaultVariableResolverProvider.class).in(Singleton.class);
        bind(RdbmsBuilder.class).toProvider(RdbmsBuilderProvider.class).in(Singleton.class);
        bind(QueryFactory.class).toProvider(QueryFactoryProvider.class).in(Singleton.class);
        bind(SelectStatementExecutor.class).toProvider(SelectStatementExecutorProvider.class).in(Singleton.class);
        bind(ModifyStatementExecutor.class).toProvider(ModifyStatementExecutorProvider.class).in(Singleton.class);

        ExtendableCoercer coercer = new DefaultCoercer();
        bind(Coercer.class).toInstance(coercer);
        bind(ExtendableCoercer.class).toInstance(coercer);
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
        bind(OperationCallInterceptorProvider.class).toProvider(OperationCallInterceptorProviderProvider.class).in(Singleton.class);

        bind(Dispatcher.class).toProvider(DefaultDispatcherProvider.class).asEagerSingleton();
        bind(Boolean.class).annotatedWith(Names.named(DISPATCHER_METRICS_RETURNED)).toInstance(Boolean.FALSE);
        bind(Boolean.class).annotatedWith(Names.named(DISPATCHER_ENABLE_VALIDATION)).toInstance(Boolean.TRUE);
        bind(Boolean.class).annotatedWith(Names.named(DISPATCHER_TRIM_STRING)).toInstance(Boolean.FALSE);
        bind(Boolean.class).annotatedWith(Names.named(DISPATCHER_CASE_INSENSITIVE_LIKE)).toInstance(Boolean.FALSE);

        // Validator
        bind(ValidatorProvider.class).toProvider(ValidatorProviderProvider.class).asEagerSingleton();
        bind(PayloadValidator.class).toProvider(DefaultPayloadValidatorProvider.class).asEagerSingleton();
        bind(String.class).annotatedWith(Names.named(PAYLOAD_VALIDATOR_REQUIRED_STRING_VALIDATOR_OPTION)).toInstance("ACCEPT_NON_EMPTY");

    }
    */


    @Provides
    @Singleton
    public AccessManager provideAccessManeger(AsmModel asmModel) {
        return DefaultAccessManager.builder()
                .asmModel(asmModel)
                .build();
    }

    @Provides
    @Singleton
    public DataTypeManager provideDataTypeManager(ExtendableCoercer coercer) {
        return new DataTypeManager(coercer);
    }

    @Provides
    @Singleton
    public IdentifierProvider<UUID> provideIdentifierProvider() {
        return new UUIDIdentifierProvider();
    }

    @Provides
    @Singleton
    @SuppressWarnings("unchecked")
    public ModifyStatementExecutor provideModifyStatementExecutor(
            AsmModel asmModel,
            RdbmsModel rdbmsModel,
            IdentifierProvider identifierProvider,
            TransformationTraceService transformationTraceService,
            RdbmsParameterMapper rdbmsParameterMapper,
            ExtendableCoercer coercer,
            RdbmsResolver rdbmsResolver
    ) {
        return ModifyStatementExecutor.builder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .identifierProvider(identifierProvider)
                .transformationTraceService(transformationTraceService)
                .rdbmsParameterMapper(rdbmsParameterMapper)
                .coercer(coercer)
                .rdbmsResolver(rdbmsResolver)
                .build();
    }

    @Provides
    @Singleton
    public PlatformTransactionManager providePlatformTransactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }


    @Provides
    @Singleton
    public QueryFactory provideQueryFactory(
            ModelHolder models,
            ExtendableCoercer coercer,
            @Nullable @Named(QUERY_FACTORY_CUSTOM_JOIN_DEFINITIONS) EMap<EReference, CustomJoinDefinition> customJoinDefinitions
    ) {

        JqlExpressionBuilderConfig jqlExpressionBuilderConfig = new JqlExpressionBuilderConfig();
        jqlExpressionBuilderConfig.setResolveOnlyCurrentLambdaScope(false);

        final AsmJqlExtractor asmJqlExtractor = new AsmJqlExtractor(models.getAsmModel().getResourceSet(),
                models.getMeasureModel().getResourceSet(), URI.createURI("expr:" + models.getAsmModel().getName()), jqlExpressionBuilderConfig);

        QueryFactory queryFactory = new QueryFactory(
                models.getAsmModel().getResourceSet(),
                models.getMeasureModel().getResourceSet(),
                asmJqlExtractor.extractExpressions(),
                coercer,
                requireNonNullElse(customJoinDefinitions, ECollections.asEMap(new ConcurrentHashMap<>())));

        return queryFactory;
    }


    @Provides
    @Singleton
    @SuppressWarnings("unchecked")
    public RdbmsBuilder provideRdbmsBuilder(
            AsmModel asmModel,
            RdbmsModel rdbmsModel,
            RdbmsResolver rdbmsResolver,
            RdbmsParameterMapper rdbmsParameterMapper,
            IdentifierProvider identifierProvider,
            Coercer coercer,
            VariableResolver variableResolver,
            MapperFactory mapperFactory,
            Dialect dialect
    ) {
        AsmUtils asm = new AsmUtils(asmModel.getResourceSet());

        return RdbmsBuilder.builder()
                .rdbmsModel(rdbmsModel)
                .ancestorNameFactory(new AncestorNameFactory(asm.all(EClass.class)))
                .descendantNameFactory(new DescendantNameFactory(asm.all(EClass.class)))
                .rdbmsResolver(rdbmsResolver)
                .parameterMapper(rdbmsParameterMapper)
                .asmUtils(asm)
                .identifierProvider(identifierProvider)
                .coercer(coercer)
                .variableResolver(variableResolver)
                .mapperFactory(mapperFactory)
                .dialect(dialect)
                .build();
    }

    @Provides
    @Singleton
    @SuppressWarnings("unchecked")
    public DAO provideDAO(
            DataSource dataSource,
            Context context,
            AsmModel asmModel,
            IdentifierProvider identifierProvider,
            InstanceCollector instanceCollector,
            MetricsCollector metricsCollector,
            @Named(RDBMS_DAO_OPTIMISTIC_LOCK_ENABLED) @Nullable Boolean optimisticLockEnabled,
            @Named(RDBMS_DAO_MARK_SELECTED_RANGE_ITEMS) @Nullable Boolean markSelectedRangeItems,
            SelectStatementExecutor selectStatementExecutor,
            ModifyStatementExecutor modifyStatementExecutor,
            QueryFactory queryFactory
    ) {
        RdbmsDAOImpl.RdbmsDAOImplBuilder builder =  RdbmsDAOImpl.builder()
                .dataSource(dataSource)
                .context(context)
                .asmModel(asmModel)
                .identifierProvider(identifierProvider)
                .instanceCollector(instanceCollector)
                .metricsCollector(metricsCollector)
                .optimisticLockEnabled(requireNonNullElse(optimisticLockEnabled, true))
                .markSelectedRangeItems(requireNonNullElse(markSelectedRangeItems, false))
                .selectStatementExecutor(selectStatementExecutor)
                .modifyStatementExecutor(modifyStatementExecutor)
                .queryFactory(queryFactory)
                ;

        return builder.build();
    }

    @Provides
    @Singleton
    @SuppressWarnings("unchecked")
    public InstanceCollector provideInstanceCollector(
            AsmModel asmModel,
            DataSource dataSource,
            RdbmsResolver rdbmsResolver,
            RdbmsModel rdbmsModel,
            Coercer coercer,
            RdbmsParameterMapper rdbmsParameterMapper,
            IdentifierProvider identifierProvider
    ) {
        InstanceCollector instanceCollector = RdbmsInstanceCollector.builder()
                .jdbcTemplate(new NamedParameterJdbcTemplate(dataSource))
                .asmModel(asmModel)
                .rdbmsResolver(rdbmsResolver)
                .rdbmsModel(rdbmsModel)
                .coercer(coercer)
                .rdbmsParameterMapper(rdbmsParameterMapper)
                .identifierProvider(identifierProvider)
                .build();
        return instanceCollector;
    }

    @Provides
    @Singleton
    public RdbmsResolver provideRdbmsResolver(
            AsmModel asmModel,
            TransformationTraceService transformationTraceService
    ) {
        return RdbmsResolver.builder()
                .asmModel(asmModel)
                .transformationTraceService(transformationTraceService)
                .build();
    }

    @Provides
    @Singleton
    @SuppressWarnings("unchecked")
    public SelectStatementExecutor prSelectStatementExecutor(
            AsmModel asmModel,
            RdbmsModel rdbmsModel,
            QueryFactory queryFactory,
            DataTypeManager dataTypeManager,
            IdentifierProvider identifierProvider,
            MetricsCollector metricsCollector,
            @Named(RDBMS_SELECT_CHUNK_SIZE) @Nullable Integer chunkSize,
            TransformationTraceService transformationTraceService,
            RdbmsParameterMapper rdbmsParameterMapper,
            RdbmsBuilder rdbmsBuilder,
            RdbmsResolver rdbmsResolver
    ) {
        return SelectStatementExecutor.builder()
                .asmModel(asmModel)
                .rdbmsModel(rdbmsModel)
                .queryFactory(queryFactory)
                .dataTypeManager(dataTypeManager)
                .identifierProvider(identifierProvider)
                .metricsCollector(metricsCollector)
                .chunkSize(requireNonNullElse(chunkSize, 1000))
                .transformationTraceService(transformationTraceService)
                .rdbmsParameterMapper(rdbmsParameterMapper)
                .rdbmsBuilder(rdbmsBuilder)
                .rdbmsResolver(rdbmsResolver)
                .build();
    }

    @Provides
    @Singleton
    public SimpleLiquibaseExecutor provideSimpleLiquibaseExecutor() {
        return new SimpleLiquibaseExecutor();
    }

    @Provides
    @Singleton
    public TransformationTraceService provideTransformationTraceService(ModelHolder models) {
        TransformationTraceService transformationTraceService = new TransformationTraceServiceImpl();
        transformationTraceService.add(models.getAsm2rdbms());
        return transformationTraceService;
    }

    @Provides
    @Singleton
    public ActorResolver provideActorResolver(
            DataTypeManager dataTypeManager,
            DAO dao,
            AsmModel asmModel,
            @Named(ACTOR_RESOLVER_CHECK_MAPPED_ACTORS) @Nullable Boolean checkMappedActors
    ) {
        return DefaultActorResolver.builder()
                .dataTypeManager(dataTypeManager)
                .dao(dao)
                .asmModel(asmModel)
                .checkMappedActors(requireNonNullElse(checkMappedActors, false))
                .build();
    }

    @Provides
    @Singleton
    @SuppressWarnings("unchecked")
    public Dispatcher providesDispatcher(
            ModelHolder models,
            DAO dao,
            IdentifierProvider identifierProvider,
            DispatcherFunctionProvider dispatcherFunctionProvider,
            OperationCallInterceptorProvider operationCallInterceptorProvider,
            PlatformTransactionManager platformTransactionManager,
            DataTypeManager dataTypeManager,
            IdentifierSigner identifierSigner,
            AccessManager accessManager,
            ActorResolver actorResolver,
            Context context,
            ValidatorProvider validatorProvider,
            PayloadValidator payloadValidator,
            MetricsCollector metricsCollector,
            OpenIdConfigurationProvider openIdConfigurationProvider,
            @Nullable TokenValidator tokenValidator,
            @Nullable TokenIssuer tokenIssuer,
            @Named(DISPATCHER_METRICS_RETURNED) @Nullable Boolean metricsReturned,
            @Named(DISPATCHER_ENABLE_VALIDATION) @Nullable Boolean enableValidation,
            @Named(DISPATCHER_TRIM_STRING) @Nullable Boolean trimString,
            @Named(DISPATCHER_CASE_INSENSITIVE_LIKE) @Nullable Boolean caseInsensitiveLike
    ) {
        return DefaultDispatcher.builder()
                .asmModel(models.getAsmModel())
                .expressionModel(models.getExpressionModel())
                .dao(dao)
                .identifierProvider(identifierProvider)
                .dispatcherFunctionProvider(dispatcherFunctionProvider)
                .operationCallInterceptorProvider(operationCallInterceptorProvider)
                .transactionManager(platformTransactionManager)
                .dataTypeManager(dataTypeManager)
                .identifierSigner(identifierSigner)
                .accessManager(accessManager)
                .actorResolver(actorResolver)
                .context(context)
                .validatorProvider(validatorProvider)
                .payloadValidator(payloadValidator)
                .metricsCollector(metricsCollector)
                .openIdConfigurationProvider(openIdConfigurationProvider)
                .filestoreTokenValidator(tokenValidator)
                .filestoreTokenIssuer(tokenIssuer)
                .metricsReturned(requireNonNullElse(metricsReturned, true))
                .enableValidation(requireNonNullElse(enableValidation, true))
                .trimString(requireNonNullElse(trimString, false))
                .caseInsensitiveLike(requireNonNullElse(caseInsensitiveLike, false))
                .build();
    }

    @Provides
    @Singleton
    public IdentifierSigner provideIdentifierSigner(
            AsmModel asmModel,
            IdentifierProvider identifierProvider,
            DataTypeManager dataTypeManager,
            @Named(IDENTIFIER_SIGNER_SECRET) @Nullable String secret
    ) {
        return DefaultIdentifierSigner.builder()
                .asmModel(asmModel)
                .identifierProvider(identifierProvider)
                .dataTypeManager(dataTypeManager)
                .secret(requireNonNullElse(secret, generateNewSecret()))
                .build();
    }

    private String generateNewSecret() {
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

    @Provides
    @Singleton
    public MetricsCollector provideMetricsCollector(
            Context context,
            @Named(METRICS_COLLECTOR_CONSUMER) @Nullable Consumer metricsConsumer,
            @Named(METRICS_COLLECTOR_ENABLED) @Nullable Boolean enabled,
            @Named(METRICS_COLLECTOR_VERBOSE) @Nullable Boolean verbose
    ) {
        return DefaultMetricsCollector.builder()
                .context(context)
                .metricsConsumer(requireNonNullElse(metricsConsumer, (m) -> {}))
                .enabled(requireNonNullElse(enabled, false))
                .verbose(requireNonNullElse(verbose, false))
                .build();
    }

    @Provides
    @Singleton
    public PayloadValidator providePayloadValidator(
            ModelHolder models,
            DataTypeManager dataTypeManager,
            IdentifierProvider identifierProvider,
            ValidatorProvider validatorProvider,
            @Named(PAYLOAD_VALIDATOR_REQUIRED_STRING_VALIDATOR_OPTION) @Nullable String requiredStringValidatorOption
    ) {
        return DefaultPayloadValidator.builder()
                .asmUtils(new AsmUtils(models.getAsmModel().getResourceSet()))
                .coercer(dataTypeManager.getCoercer())
                .identifierProvider(identifierProvider)
                .validatorProvider(validatorProvider)
                .requiredStringValidatorOption(
                        DefaultPayloadValidator.RequiredStringValidatorOption.valueOf(Objects.requireNonNullElse(requiredStringValidatorOption, "ACCEPT_NON_EMPTY")))
                .build();
    }

    @Provides
    @Singleton
    public VariableResolver provideVariableResolver(
            DataTypeManager dataTypeManager,
            Context context,
            Sequence sequence
    ) {
        DefaultVariableResolver variableResolver = new DefaultVariableResolver(dataTypeManager, context);
        variableResolver.registerSupplier("SYSTEM", "current_timestamp", new CurrentTimestampProvider(), false);
        variableResolver.registerSupplier("SYSTEM", "current_date", new CurrentDateProvider(), false);
        variableResolver.registerSupplier("SYSTEM", "current_time", new CurrentTimeProvider(), false);
        variableResolver.registerFunction("ENVIRONMENT", new EnvironmentVariableProvider(), true);
        variableResolver.registerFunction("SEQUENCE", new SequenceProvider(sequence), false);
        return variableResolver;
    }

    @Provides
    @Singleton
    public DispatcherFunctionProvider provideDispatcherFunctionProvider() {
        final EMap<EOperation, Function<Payload, Payload>> scripts = new BasicEMap<>();
        final EMap<EOperation, Function<Payload, Payload>> sdkFunctions = new BasicEMap<>();

        DispatcherFunctionProvider dispatcherFunctionProvider = new DispatcherFunctionProvider() {
            @Override
            public EMap<EOperation, Function<Payload, Payload>> getSdkFunctions() {
                return sdkFunctions;
            }

            @Override
            public EMap<EOperation, Function<Payload, Payload>> getScriptFunctions() {
                return scripts;
            }
        };
        return dispatcherFunctionProvider;
    }

    @Provides
    @Singleton
    public OperationCallInterceptorProvider provideOperationCallInterceptorProvider() {
        final EList<OperationCallInterceptor> interceptors = new BasicEList<>();

        return new OperationCallInterceptorProvider() {
            @Override
            public EList<OperationCallInterceptor> getCallOperationInterceptors() {
                return interceptors;
            }
        };
    }

    @Provides
    @Singleton
    public Context provideContext(
            @Named(THREAD_CONTEXT_DEBUG_THREAD_FORK) @Nullable Boolean debugThreadFork,
            @Named(THREAD_CONTEXT_INHERITABLE_CONTEXT) @Nullable Boolean inheritableContext,
            DataTypeManager dataTypeManager
    ) {
        return new ThreadContext(requireNonNullElse(debugThreadFork, false), requireNonNullElse(inheritableContext, true), dataTypeManager);
    }

    @Provides
    @Singleton
    public ValidatorProvider provideValidationProvider(
            DAO dao,
            IdentifierProvider identifierProvider,
            AsmModel asmModel,
            Context context
    ) {
        return new DefaultValidatorProvider(dao, identifierProvider, asmModel, context);
    }

    @Provides
    @Singleton
    public AsmModel asmModel(ModelHolder modelHolder) {
        return modelHolder.getAsmModel();
    }

    @Provides
    @Singleton
    public ExpressionModel expressionModel(ModelHolder modelHolder) {
        return modelHolder.getExpressionModel();
    }

    @Provides
    @Singleton
    public RdbmsModel rdbmsModel(ModelHolder modelHolder) {
        return modelHolder.getRdbmsModel();
    }

    @Provides
    @Singleton
    public MeasureModel measureModel(ModelHolder modelHolder) {
        return modelHolder.getMeasureModel();
    }

    @Provides
    @Singleton
    public LiquibaseModel liquibaseModel(ModelHolder modelHolder) {
        return modelHolder.getLiquibaseModel();
    }

    @Provides
    @Singleton
    public Asm2RdbmsTransformationTrace asm2RdbmsTransformationTrace(ModelHolder modelHolder) {
        return modelHolder.getAsm2rdbms();
    }

    @Provides
    @Singleton
    public PasswordPolicy providesPasswordPolicy() {
        return new NoPasswordPolicy();
    }

    @Provides
    @Singleton
    public RealmExtractor provideRealmExtractor(AsmModel asmModel) {
        return new PathInfoRealmExtractor(asmModel);
    }

}
