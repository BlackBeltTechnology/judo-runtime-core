package hu.blackbelt.judo.runtime.core.guice;

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

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.PayloadValidator;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.meta.keycloak.runtime.KeycloakModel;
import hu.blackbelt.judo.meta.liquibase.runtime.LiquibaseModel;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AccessManager;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AuthenticationInterceptorProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.UnsupportedExportImpl;
import hu.blackbelt.judo.runtime.core.guice.accessmanager.DefaultAccessManagerProvider;
import hu.blackbelt.judo.runtime.core.guice.accessmanager.DefaultAuthenticationInterceptorProviderProvider;
import hu.blackbelt.judo.runtime.core.guice.core.CoercererProvider;
import hu.blackbelt.judo.runtime.core.guice.core.DataTypeManagerProvider;
import hu.blackbelt.judo.runtime.core.guice.core.ExtendableCoercererProvider;
import hu.blackbelt.judo.runtime.core.guice.core.UUIDIdentifierProviderProvider;
import hu.blackbelt.judo.runtime.core.guice.dao.rdbms.*;
import hu.blackbelt.judo.runtime.core.guice.dispatcher.*;
import hu.blackbelt.osgi.i18n.api.LocaleProvider;
import hu.blackbelt.judo.runtime.core.dao.core.collectors.InstanceCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.ModifyStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.SelectStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dispatcher.DispatcherFunctionProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.Export;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;
import hu.blackbelt.judo.runtime.core.query.CustomJoinDefinition;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.runtime.core.validator.ValidatorProvider;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import lombok.*;
import org.eclipse.emf.ecore.EReference;
import org.springframework.transaction.PlatformTransactionManager;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.function.Consumer;

public class JudoDefaultModule extends AbstractModule {

    @Getter
    private JudoDefaultModuleConfiguration configuration = JudoDefaultModuleConfiguration.builder().build();

    public static class JudoDefaultModuleBuilder {
        JudoDefaultModuleConfiguration configuration = configuration = null;
        Object injectModulesTo = JudoDefaultModuleConfiguration.DEFAULT.getInjectModulesTo();
        JudoModelLoader judoModelLoader = JudoDefaultModuleConfiguration.DEFAULT.getJudoModelLoader();
        Boolean bindModelHolder = JudoDefaultModuleConfiguration.DEFAULT.getBindModelHolder();
        Map<EReference, CustomJoinDefinition> queryFactoryCustomJoinDefinitions = JudoDefaultModuleConfiguration.DEFAULT.getQueryFactoryCustomJoinDefinitions();
        Boolean rdbmsDaoOptimisticLockEnabled = JudoDefaultModuleConfiguration.DEFAULT.getRdbmsDaoOptimisticLockEnabled();
        Integer rdbmsDaoChunkSize = JudoDefaultModuleConfiguration.DEFAULT.getRdbmsDaoChunkSize();
        Integer rdbmsDaoMaximumRecursionCount = JudoDefaultModuleConfiguration.DEFAULT.getRdbmsDaoMaximumRecursionCount();
        Boolean actorResolverCheckMappedActors = JudoDefaultModuleConfiguration.DEFAULT.getActorResolverCheckMappedActors();
        String actorResolverAcceptableClients = JudoDefaultModuleConfiguration.DEFAULT.getActorResolverAcceptableClients();
        Boolean dispatcherMetricsReturned = JudoDefaultModuleConfiguration.DEFAULT.getDispatcherMetricsReturned();
        Boolean dispatcherEnableDefaultValidation = JudoDefaultModuleConfiguration.DEFAULT.getDispatcherEnableDefaultValidation();
        Boolean dispatcherCaseInsensitiveLike = JudoDefaultModuleConfiguration.DEFAULT.getDispatcherCaseInsensitiveLike();
        Boolean dispatcherTrimString = JudoDefaultModuleConfiguration.DEFAULT.getDispatcherTrimString();
        String identifierSignerSecret = JudoDefaultModuleConfiguration.DEFAULT.getIdentifierSignerSecret();
        Consumer metricsCollectorConsumer = JudoDefaultModuleConfiguration.DEFAULT.getMetricsCollectorConsumer();
        Boolean metricsCollectorEnabled = JudoDefaultModuleConfiguration.DEFAULT.getMetricsCollectorEnabled();
        Boolean metricsCollectorVerbose = JudoDefaultModuleConfiguration.DEFAULT.getMetricsCollectorVerbose();
        String payloadValidatorRequiredStringValidatorOption = JudoDefaultModuleConfiguration.DEFAULT.getPayloadValidatorRequiredStringValidatorOption();
        Boolean threadContextDebugThreadFork = JudoDefaultModuleConfiguration.DEFAULT.getThreadContextDebugThreadFork();
        Boolean threadContextInheritableContext = JudoDefaultModuleConfiguration.DEFAULT.getThreadContextInheritableContext();
        Long rdbmsSequenceStart = JudoDefaultModuleConfiguration.DEFAULT.getRdbmsSequenceStart();
        Long rdbmsSequenceIncrement = JudoDefaultModuleConfiguration.DEFAULT.getRdbmsSequenceIncrement();
        Boolean rdbmsSequenceCreateIfNotExists = JudoDefaultModuleConfiguration.DEFAULT.getRdbmsSequenceCreateIfNotExists();
        RdbmsResolver RdbmsResolver = JudoDefaultModuleConfiguration.DEFAULT.getRdbmsResolver();
        VariableResolver VariableResolver = JudoDefaultModuleConfiguration.DEFAULT.getVariableResolver();
        RdbmsBuilder RdbmsBuilder = JudoDefaultModuleConfiguration.DEFAULT.getRdbmsBuilder();
        QueryFactory QueryFactory = JudoDefaultModuleConfiguration.DEFAULT.getQueryFactory();
        SelectStatementExecutor SelectStatementExecutor = JudoDefaultModuleConfiguration.DEFAULT.getSelectStatementExecutor();
        ModifyStatementExecutor ModifyStatementExecutor = JudoDefaultModuleConfiguration.DEFAULT.getModifyStatementExecutor();
        ExtendableCoercer ExtendableCoercer = JudoDefaultModuleConfiguration.DEFAULT.getExtendableCoercer();
        DataTypeManager DataTypeManager = JudoDefaultModuleConfiguration.DEFAULT.getDataTypeManager();
        IdentifierProvider IdentifierProvider = JudoDefaultModuleConfiguration.DEFAULT.getIdentifierProvider();
        IdentifierSigner IdentifierSigner = JudoDefaultModuleConfiguration.DEFAULT.getIdentifierSigner();
        AccessManager AccessManager = JudoDefaultModuleConfiguration.DEFAULT.getAccessManager();
        AuthenticationInterceptorProvider AuthenticationInterceptorProvider = JudoDefaultModuleConfiguration.DEFAULT.getAuthenticationInterceptorProvider();
        Context Context = JudoDefaultModuleConfiguration.DEFAULT.getContext();
        MetricsCollector MetricsCollector = JudoDefaultModuleConfiguration.DEFAULT.getMetricsCollector();
        TransformationTraceService TransformationTraceService = JudoDefaultModuleConfiguration.DEFAULT.getTransformationTraceService();
        InstanceCollector InstanceCollector = JudoDefaultModuleConfiguration.DEFAULT.getInstanceCollector();
        DAO DAO = JudoDefaultModuleConfiguration.DEFAULT.getDao();
        ActorResolver ActorResolver = JudoDefaultModuleConfiguration.DEFAULT.getActorResolver();
        DispatcherFunctionProvider DispatcherFunctionProvider = JudoDefaultModuleConfiguration.DEFAULT.getDispatcherFunctionProvider();
        OperationCallInterceptorProvider OperationCallInterceptorProvider = JudoDefaultModuleConfiguration.DEFAULT.getOperationCallInterceptorProvider();
        Dispatcher Dispatcher = JudoDefaultModuleConfiguration.DEFAULT.getDispatcher();
        ValidatorProvider ValidatorProvider = JudoDefaultModuleConfiguration.DEFAULT.getValidatorProvider();
        PayloadValidator PayloadValidator = JudoDefaultModuleConfiguration.DEFAULT.getPayloadValidator();
        Export Export = JudoDefaultModuleConfiguration.DEFAULT.getExport();
        PlatformTransactionManager PlatformTransactionManager = JudoDefaultModuleConfiguration.DEFAULT.getPlatformTransactionManager();
    }

    @Builder
    public JudoDefaultModule(
                            JudoDefaultModuleConfiguration configuration,
                            Object injectModulesTo,
                            JudoModelLoader judoModelLoader,
                            Boolean bindModelHolder,
                            Map<EReference, CustomJoinDefinition> queryFactoryCustomJoinDefinitions,
                            Boolean rdbmsDaoOptimisticLockEnabled,
                            Integer rdbmsDaoChunkSize,
                            Integer rdbmsDaoMaximumRecursionCount,
                            Boolean actorResolverCheckMappedActors,
                            String actorResolverAcceptableClients,
                            String actorResolverPrincipalLocaleAttribute,
                            String actorResolverSupportedLanguages,
                            String actorResolverDefaultLanguage,
                            Boolean actorResolverBrowserLanguageCheck,
                            Boolean dispatcherMetricsReturned,
                            Boolean dispatcherEnableDefaultValidation,
                            Boolean dispatcherTrimString,
                            Boolean dispatcherCaseInsensitiveLike,
                            String identifierSignerSecret,
                            Consumer metricsCollectorConsumer,
                            Boolean metricsCollectorEnabled,
                            Boolean metricsCollectorVerbose,
                            String payloadValidatorRequiredStringValidatorOption,
                            Boolean threadContextDebugThreadFork,
                            Boolean threadContextInheritableContext,
                            Long rdbmsSequenceStart,
                            Long rdbmsSequenceIncrement,
                            Boolean rdbmsSequenceCreateIfNotExists,
                            RdbmsResolver rdbmsResolver,
                            VariableResolver variableResolver,
                            RdbmsBuilder rdbmsBuilder,
                            QueryFactory queryFactory,
                            SelectStatementExecutor selectStatementExecutor,
                            ModifyStatementExecutor modifyStatementExecutor,
                            ExtendableCoercer extendableCoercer,
                            DataTypeManager dataTypeManager,
                            IdentifierProvider identifierProvider,
                            IdentifierSigner identifierSigner,
                            AccessManager accessManager,
                            AuthenticationInterceptorProvider authenticationInterceptorProvider,
                            Context context,
                            MetricsCollector metricsCollector,
                            TransformationTraceService transformationTraceService,
                            InstanceCollector instanceCollector,
                            DAO dao,
                            ActorResolver actorResolver,
                            DispatcherFunctionProvider dispatcherFunctionProvider,
                            OperationCallInterceptorProvider operationCallInterceptorProvider,
                            Dispatcher dispatcher,
                            ValidatorProvider validatorProvider,
                            PayloadValidator payloadValidator,
                            Export export,
                            PlatformTransactionManager platformTransactionManager
         ) {
        if (configuration != null) {
            this.configuration = configuration;
        } else {
            this.configuration = JudoDefaultModuleConfiguration.builder()
                    .injectModulesTo(injectModulesTo)
                    .judoModelLoader(judoModelLoader)
                    .bindModelHolder(bindModelHolder)
                    .queryFactoryCustomJoinDefinitions(queryFactoryCustomJoinDefinitions)
                    .rdbmsDaoOptimisticLockEnabled(rdbmsDaoOptimisticLockEnabled)
                    .rdbmsDaoChunkSize(rdbmsDaoChunkSize)
                    .rdbmsDaoMaximumRecursionCount(rdbmsDaoMaximumRecursionCount)
                    .actorResolverCheckMappedActors(actorResolverCheckMappedActors)
                    .actorResolverAcceptableClients(actorResolverAcceptableClients)
                    .actorResolverPrincipalLocaleAttribute(actorResolverPrincipalLocaleAttribute)
                    .actorResolverSupportedLanguages(actorResolverSupportedLanguages)
                    .actorResolverDefaultLanguage(actorResolverDefaultLanguage)
                    .actorResolverBrowserLanguageCheck(actorResolverBrowserLanguageCheck)
                    .dispatcherMetricsReturned(dispatcherMetricsReturned)
                    .dispatcherEnableDefaultValidation(dispatcherEnableDefaultValidation)
                    .dispatcherTrimString(dispatcherTrimString)
                    .dispatcherCaseInsensitiveLike(dispatcherCaseInsensitiveLike)
                    .identifierSignerSecret(identifierSignerSecret)
                    .metricsCollectorConsumer(metricsCollectorConsumer)
                    .metricsCollectorEnabled(metricsCollectorEnabled)
                    .metricsCollectorVerbose(metricsCollectorVerbose)
                    .payloadValidatorRequiredStringValidatorOption(payloadValidatorRequiredStringValidatorOption)
                    .threadContextDebugThreadFork(threadContextDebugThreadFork)
                    .threadContextInheritableContext(threadContextInheritableContext)
                    .rdbmsSequenceStart(rdbmsSequenceStart)
                    .rdbmsSequenceIncrement(rdbmsSequenceIncrement)
                    .rdbmsSequenceCreateIfNotExists(rdbmsSequenceCreateIfNotExists)
                    .rdbmsResolver(rdbmsResolver)
                    .variableResolver(variableResolver)
                    .rdbmsBuilder(rdbmsBuilder)
                    .queryFactory(queryFactory)
                    .selectStatementExecutor(selectStatementExecutor)
                    .modifyStatementExecutor(modifyStatementExecutor)
                    .extendableCoercer(extendableCoercer)
                    .dataTypeManager(dataTypeManager)
                    .identifierProvider(identifierProvider)
                    .identifierSigner(identifierSigner)
                    .accessManager(accessManager)
                    .authenticationInterceptorProvider(authenticationInterceptorProvider)
                    .context(context)
                    .metricsCollector(metricsCollector)
                    .transformationTraceService(transformationTraceService)
                    .instanceCollector(instanceCollector)
                    .dao(dao)
                    .actorResolver(actorResolver)
                    .dispatcherFunctionProvider(dispatcherFunctionProvider)
                    .operationCallInterceptorProvider(operationCallInterceptorProvider)
                    .dispatcher(dispatcher)
                    .validatorProvider(validatorProvider)
                    .payloadValidator(payloadValidator)
                    .export(export)
                    .platformTransactionManager(platformTransactionManager)
                    .build();
        }
    }

    public static String generateNewSecret() {
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

    protected void configureModels() {
        bind(AsmModel.class).toInstance(configuration.getJudoModelLoader().getAsmModel());
        bind(RdbmsModel.class).toInstance(configuration.getJudoModelLoader().getRdbmsModel());
        bind(MeasureModel.class).toInstance(configuration.getJudoModelLoader().getMeasureModel());
        bind(LiquibaseModel.class).toInstance(configuration.getJudoModelLoader().getLiquibaseModel());
        bind(ExpressionModel.class).toInstance(configuration.getJudoModelLoader().getExpressionModel());
        if (configuration.getJudoModelLoader().getKeycloakModel() != null) {
            bind(KeycloakModel.class).toInstance(configuration.getJudoModelLoader().getKeycloakModel());
        }
        // Model
        if (configuration.getBindModelHolder()) {
            bind(JudoModelLoader.class).toInstance(configuration.getJudoModelLoader());
        }
    }

    protected void configureOptions() {
        bind(Map.class).annotatedWith(JudoConfigurationQualifiers.QueryFactoryCustomJoinDefinitions.class).toInstance(configuration.getQueryFactoryCustomJoinDefinitions());
        bind(Boolean.class).annotatedWith(JudoConfigurationQualifiers.RdbmsDaoOptimisticLockEnabled.class).toInstance(configuration.getRdbmsDaoOptimisticLockEnabled());
        bind(Integer.class).annotatedWith(JudoConfigurationQualifiers.RdbmsDaoChunkSize.class).toInstance(configuration.getRdbmsDaoChunkSize());
        bind(Integer.class).annotatedWith(JudoConfigurationQualifiers.RdbmsDaoMaximumRecursionCount.class).toInstance(configuration.getRdbmsDaoMaximumRecursionCount());
        bind(Boolean.class).annotatedWith(JudoConfigurationQualifiers.ActorResolverCheckMappedActors.class).toInstance(configuration.getActorResolverCheckMappedActors());
        if (configuration.getActorResolverAcceptableClients() != null) {
            bind(String.class).annotatedWith(JudoConfigurationQualifiers.ActorResolverAcceptableClients.class).toInstance(configuration.getActorResolverAcceptableClients());
        }
        if (configuration.getActorResolverPrincipalLocaleAttribute() != null) {
            bind(String.class).annotatedWith(JudoConfigurationQualifiers.ActorResolverPrincipalLocaleAttribute.class).toInstance(configuration.getActorResolverPrincipalLocaleAttribute());
        }
        if (configuration.getActorResolverSupportedLanguages() != null) {
            bind(String.class).annotatedWith(JudoConfigurationQualifiers.ActorResolverSupportedLanguages.class).toInstance(configuration.getActorResolverSupportedLanguages());
        }
        if (configuration.getActorResolverDefaultLanguage() != null) {
            bind(String.class).annotatedWith(JudoConfigurationQualifiers.ActorResolverDefaultLanguage.class).toInstance(configuration.getActorResolverDefaultLanguage());
        }
        bind(Boolean.class).annotatedWith(JudoConfigurationQualifiers.ActorResolverBrowserLanguageCheck.class).toInstance(configuration.getActorResolverBrowserLanguageCheck() != null ? configuration.getActorResolverBrowserLanguageCheck() : Boolean.TRUE);
        bind(Boolean.class).annotatedWith(JudoConfigurationQualifiers.DispatcherMetricsReturned.class).toInstance(configuration.getDispatcherMetricsReturned());
        bind(Boolean.class).annotatedWith(JudoConfigurationQualifiers.DispatcherEnableDefaultValidation.class).toInstance(configuration.getDispatcherEnableDefaultValidation());
        bind(Boolean.class).annotatedWith(JudoConfigurationQualifiers.DispatcherTrimString.class).toInstance(configuration.getDispatcherTrimString());
        bind(Boolean.class).annotatedWith(JudoConfigurationQualifiers.DispatcherCaseInsensitiveLike.class).toInstance(configuration.getDispatcherCaseInsensitiveLike());
        bind(String.class).annotatedWith(JudoConfigurationQualifiers.IdentifierSignerSecret.class).toInstance(configuration.getIdentifierSignerSecret() != null ? configuration.getIdentifierSignerSecret() : generateNewSecret());
        bind(Consumer.class).annotatedWith(JudoConfigurationQualifiers.MetricsCollectorConsumer.class).toInstance(configuration.getMetricsCollectorConsumer());
        bind(Boolean.class).annotatedWith(JudoConfigurationQualifiers.MetricsCollectorEnabled.class).toInstance(configuration.getMetricsCollectorEnabled());
        bind(Boolean.class).annotatedWith(JudoConfigurationQualifiers.MetricsCollectorVerbose.class).toInstance(configuration.getMetricsCollectorVerbose());
        bind(String.class).annotatedWith(JudoConfigurationQualifiers.PayloadValidatorRequiredStringValidatorOption.class).toInstance(configuration.getPayloadValidatorRequiredStringValidatorOption());
        bind(Boolean.class).annotatedWith(JudoConfigurationQualifiers.ThreadContextDebugThreadFork.class).toInstance(configuration.getThreadContextDebugThreadFork());
        bind(Boolean.class).annotatedWith(JudoConfigurationQualifiers.ThreadContextInheritableContext.class).toInstance(configuration.getThreadContextInheritableContext());
        bind(Long.class).annotatedWith(JudoConfigurationQualifiers.RdbmsSequenceStart.class).toInstance(configuration.getRdbmsSequenceStart());
        bind(Long.class).annotatedWith(JudoConfigurationQualifiers.RdbmsSequenceIncrement.class).toInstance(configuration.getRdbmsSequenceIncrement());
        bind(Boolean.class).annotatedWith(JudoConfigurationQualifiers.RdbmsSequenceCreateIfNotExists.class).toInstance(configuration.getRdbmsSequenceCreateIfNotExists());
    }

    protected void configureRdbmsResolver() {
        if (configuration.getRdbmsResolver() != null) {
            bind(RdbmsResolver.class).toInstance(configuration.getRdbmsResolver());
        } else {
            bind(RdbmsResolver.class).toProvider(RdbmsResolverProvider.class).in(Singleton.class);
        }
    }

    protected void configureVariableResolver() {
        if (configuration.getVariableResolver() != null) {
            bind(VariableResolver.class).toInstance(configuration.getVariableResolver());
        } else {
            bind(VariableResolver.class).toProvider(DefaultVariableResolverProvider.class).in(Singleton.class);
        }
    }

    protected void configureRdbmsBuilder() {
        if (configuration.getRdbmsBuilder() != null) {
            bind(RdbmsBuilder.class).toInstance(configuration.getRdbmsBuilder());
        } else {
            bind(RdbmsBuilder.class).toProvider(RdbmsBuilderProvider.class).in(Singleton.class);
        }
    }

    protected void configureQueryFactory() {
        if (configuration.getQueryFactory() != null) {
            bind(QueryFactory.class).toInstance(configuration.getQueryFactory());
        } else {
            bind(QueryFactory.class).toProvider(QueryFactoryProvider.class).in(Singleton.class);
        }
    }

    protected void configureSelectStatementExecutor() {
        if (configuration.getSelectStatementExecutor() != null) {
            bind(SelectStatementExecutor.class).toInstance(configuration.getSelectStatementExecutor());
        } else {
            bind(SelectStatementExecutor.class).toProvider(SelectStatementExecutorProvider.class).in(Singleton.class);
        }
    }

    protected void configureModifyStatementExecutor() {
        if (configuration.getModifyStatementExecutor() != null) {
            bind(ModifyStatementExecutor.class).toInstance(configuration.getModifyStatementExecutor());
        } else {
            bind(ModifyStatementExecutor.class).toProvider(ModifyStatementExecutorProvider.class).in(Singleton.class);
        }
    }

    protected void configureExtendableCoercer() {
        if (configuration.getExtendableCoercer() != null) {
            bind(ExtendableCoercer.class).toInstance(configuration.getExtendableCoercer());
            bind(Coercer.class).toInstance(configuration.getExtendableCoercer());
        } else {
            bind(ExtendableCoercer.class).toProvider(ExtendableCoercererProvider.class).asEagerSingleton();
            bind(Coercer.class).toProvider(CoercererProvider.class).in(Singleton.class);
        }
    }

    protected void configureDataTypeManager() {
        if (configuration.getDataTypeManager() != null) {
            bind(DataTypeManager.class).toInstance(configuration.getDataTypeManager());
        } else {
            bind(DataTypeManager.class).toProvider(DataTypeManagerProvider.class).in(Singleton.class);
        }
    }

    protected void configureIdentifierProvider() {
        if (configuration.getIdentifierProvider() != null) {
            bind(IdentifierProvider.class).toInstance(configuration.getIdentifierProvider());
        } else {
            bind(IdentifierProvider.class).toProvider(UUIDIdentifierProviderProvider.class).in(Singleton.class);
        }
    }

    protected void configureIdentifierSigner() {
        if (configuration.getIdentifierSigner() != null) {
            bind(IdentifierSigner.class).toInstance(configuration.getIdentifierSigner());
        } else {
            bind(IdentifierSigner.class).toProvider(DefaultIdentifierSignerProvider.class).in(Singleton.class);
        }
    }

    protected void configureAccessManager() {
        if (configuration.getAccessManager() != null) {
            bind(AccessManager.class).toInstance(configuration.getAccessManager());
        } else {
            bind(AccessManager.class).toProvider(DefaultAccessManagerProvider.class).asEagerSingleton();
        }
    }

    protected void configureAuthenticationInterceptorProvider() {
        if (configuration.getAuthenticationInterceptorProvider() != null) {
            bind(AuthenticationInterceptorProvider.class).toInstance(configuration.getAuthenticationInterceptorProvider());
        } else {
            bind(AuthenticationInterceptorProvider.class).toProvider(DefaultAuthenticationInterceptorProviderProvider.class);
        }
    }

    protected void configureContext() {
        if (configuration.getContext() != null) {
            bind(Context.class).toInstance(configuration.getContext());
        } else {
            bind(Context.class).toProvider(ThreadContextProvider.class).in(Singleton.class);
        }
    }

    protected void configureMetricsCollector() {
        if (configuration.getMetricsCollector() != null) {
            bind(MetricsCollector.class).toInstance(configuration.getMetricsCollector());
        } else {
            bind(MetricsCollector.class).toProvider(DefaultMetricsCollectorProvider.class).in(Singleton.class);
        }
    }

    protected void configureTransformationTraceService() {
        if (configuration.getTransformationTraceService() != null) {
            bind(TransformationTraceService.class).toInstance(configuration.getTransformationTraceService());
        } else {
            bind(TransformationTraceService.class).toProvider(TransformationTraceServiceProvider.class).in(Singleton.class);
        }
    }

    protected void configureInstanceCollector() {
        if (configuration.getInstanceCollector() != null) {
            bind(InstanceCollector.class).toInstance(configuration.getInstanceCollector());
        } else {
            bind(InstanceCollector.class).toProvider(RdbmsInstanceCollectorProvider.class).in(Singleton.class);
        }
    }

    protected void configureDAO() {
        if (configuration.getDao() != null) {
            bind(DAO.class).toInstance(configuration.getDao());
        } else {
            bind(DAO.class).toProvider(RdbmsDAOProvider.class).in(Singleton.class);
        }
    }

    protected void configureActorResolver() {
        if (configuration.getActorResolver() != null) {
            bind(ActorResolver.class).toInstance(configuration.getActorResolver());
        } else {
            bind(ActorResolver.class).toProvider(DefaultActorResolverProvider.class).in(Singleton.class);
        }
    }

    protected void configureDispatcherFunctionProvider() {
        if (configuration.getDispatcherFunctionProvider() != null) {
            bind(DispatcherFunctionProvider.class).toInstance(configuration.getDispatcherFunctionProvider());
        } else {
            bind(DispatcherFunctionProvider.class).toProvider(DispatcherFunctionProviderProvider.class).in(Singleton.class);
        }
    }

    protected void configureOperationCallInterceptorProvider() {
        if (configuration.getOperationCallInterceptorProvider() != null) {
            bind(OperationCallInterceptorProvider.class).toInstance(configuration.getOperationCallInterceptorProvider());
        } else {
            bind(OperationCallInterceptorProvider.class).toProvider(OperationCallInterceptorProviderProvider.class).in(Singleton.class);
        }
    }

    protected void configureDispatcher() {
        if (configuration.getDispatcher() != null) {
            bind(Dispatcher.class).toInstance(configuration.getDispatcher());
        } else {
            bind(Dispatcher.class).toProvider(DefaultDispatcherProvider.class).asEagerSingleton();
        }
    }

    protected void configureValidatorProvider() {
        if (configuration.getValidatorProvider() != null) {
            bind(ValidatorProvider.class).toInstance(configuration.getValidatorProvider());
        } else {
            bind(ValidatorProvider.class).toProvider(ValidatorProviderProvider.class).in(Singleton.class);
        }
    }

    protected void configurePayloadValidator() {
        if (configuration.getPayloadValidator() != null) {
            bind(PayloadValidator.class).toInstance(configuration.getPayloadValidator());
        } else {
            bind(PayloadValidator.class).toProvider(DefaultPayloadValidatorProvider.class).in(Singleton.class);
        }
    }

    protected void configureExport() {
        if (configuration.getExport() != null) {
            bind(Export.class).toInstance(configuration.getExport());
        } else {
            bind(Export.class).to(UnsupportedExportImpl.class);
        }
    }

    protected void configureLocaleProvider() {
        bind(LocaleProvider.class).toProvider(PrincipalLocaleProviderProvider.class).in(Singleton.class);
    }

    protected void configurePlatformTransactionManager() {
        if (configuration.getPlatformTransactionManager() != null) {
            bind(PlatformTransactionManager.class).toInstance(configuration.getPlatformTransactionManager());
        } else {
            bind(PlatformTransactionManager.class).toProvider(PlatformTransactionManagerProvider.class).in(Singleton.class);
        }
    }

    protected void configure() {
        if (configuration.getInjectModulesTo() != null) {
            requestInjection(configuration.getInjectModulesTo());
        }
        configureModels();
        configureOptions();
        configureRdbmsResolver();
        configureVariableResolver();
        configureRdbmsBuilder();
        configureQueryFactory();
        configureSelectStatementExecutor();
        configureModifyStatementExecutor();
        configureExtendableCoercer();
        configureDataTypeManager();
        configureIdentifierProvider();
        configureIdentifierSigner();
        configureAccessManager();
        configureAuthenticationInterceptorProvider();
        configureContext();
        configureMetricsCollector();
        configureTransformationTraceService();
        configureInstanceCollector();
        configureDAO();
        configureActorResolver();
        configureLocaleProvider();
        configureDispatcherFunctionProvider();
        configureOperationCallInterceptorProvider();
        configureDispatcher();
        configureValidatorProvider();
        configurePayloadValidator();
        configureExport();
    }
}
