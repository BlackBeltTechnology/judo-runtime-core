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
        Boolean rdbmsDaoMarkSelectedRangeItems = JudoDefaultModuleConfiguration.DEFAULT.getRdbmsDaoMarkSelectedRangeItems();
        Integer rdbmsDaoChunkSize = JudoDefaultModuleConfiguration.DEFAULT.getRdbmsDaoChunkSize();
        Boolean actorResolverCheckMappedActors = JudoDefaultModuleConfiguration.DEFAULT.getActorResolverCheckMappedActors();
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
    }

    @Builder
    public JudoDefaultModule(
                            JudoDefaultModuleConfiguration configuration,
                            Object injectModulesTo,
                            JudoModelLoader judoModelLoader,
                            Boolean bindModelHolder,
                            Map<EReference, CustomJoinDefinition> queryFactoryCustomJoinDefinitions,
                            Boolean rdbmsDaoOptimisticLockEnabled,
                            Boolean rdbmsDaoMarkSelectedRangeItems,
                            Integer rdbmsDaoChunkSize,
                            Boolean actorResolverCheckMappedActors,
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
                            Boolean rdbmsSequenceCreateIfNotExists
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
                    .rdbmsDaoMarkSelectedRangeItems(rdbmsDaoMarkSelectedRangeItems)
                    .rdbmsDaoChunkSize(rdbmsDaoChunkSize)
                    .actorResolverCheckMappedActors(actorResolverCheckMappedActors)
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

        // Model
        if (configuration.getBindModelHolder()) {
            bind(JudoModelLoader.class).toInstance(configuration.getJudoModelLoader());
        }
    }

    protected void configureOptions() {
        bind(Map.class).annotatedWith(JudoConfigurationQualifiers.QueryFactoryCustomJoinDefinitions.class).toInstance(configuration.getQueryFactoryCustomJoinDefinitions());
        bind(Boolean.class).annotatedWith(JudoConfigurationQualifiers.RdbmsDaoOptimisticLockEnabled.class).toInstance(configuration.getRdbmsDaoOptimisticLockEnabled());
        bind(Boolean.class).annotatedWith(JudoConfigurationQualifiers.RdbmsDaoMarkSelectedRangeItems.class).toInstance(configuration.getRdbmsDaoMarkSelectedRangeItems());
        bind(Integer.class).annotatedWith(JudoConfigurationQualifiers.RdbmsDaoChunkSize.class).toInstance(configuration.getRdbmsDaoChunkSize());
        bind(Boolean.class).annotatedWith(JudoConfigurationQualifiers.ActorResolverCheckMappedActors.class).toInstance(configuration.getActorResolverCheckMappedActors());
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
        bind(RdbmsResolver.class).toProvider(RdbmsResolverProvider.class).in(Singleton.class);
    }

    protected void configureVariableResolver() {
        bind(VariableResolver.class).toProvider(DefaultVariableResolverProvider.class).in(Singleton.class);
    }

    protected void configureRdbmsBuilder() {
        bind(RdbmsBuilder.class).toProvider(RdbmsBuilderProvider.class).in(Singleton.class);
    }

    protected void configureQueryFactory() {
        bind(QueryFactory.class).toProvider(QueryFactoryProvider.class).in(Singleton.class);
    }

    protected void configureSelectStatementExecutor() {
        bind(SelectStatementExecutor.class).toProvider(SelectStatementExecutorProvider.class).in(Singleton.class);
    }

    protected void configureModifyStatementExecutor() {
        bind(ModifyStatementExecutor.class).toProvider(ModifyStatementExecutorProvider.class).in(Singleton.class);
    }

    protected void configureExtendableCoercer() {
        bind(ExtendableCoercer.class).toProvider(ExtendableCoercererProvider.class).asEagerSingleton();
        bind(Coercer.class).toProvider(CoercererProvider.class).in(Singleton.class);
    }

    protected void configureDataTypeManager() {
        bind(DataTypeManager.class).toProvider(DataTypeManagerProvider.class).in(Singleton.class);
    }

    protected void configureIdentifierProvider() {
        bind(IdentifierProvider.class).toProvider(UUIDIdentifierProviderProvider.class).in(Singleton.class);
    }

    protected void configureIdentifierSigner() {
        bind(IdentifierSigner.class).toProvider(DefaultIdentifierSignerProvider.class).in(Singleton.class);
    }

    protected void configureAccessManager() {
        bind(AccessManager.class).toProvider(DefaultAccessManagerProvider.class);
    }

    protected void configureAuthenticationInterceptorProvider() {
        bind(AuthenticationInterceptorProvider.class).toProvider(DefaultAuthenticationInterceptorProviderProvider.class);
    }

    protected void configureContext() {
        bind(Context.class).toProvider(ThreadContextProvider.class).in(Singleton.class);
    }

    protected void configureMetricsCollector() {
        bind(MetricsCollector.class).toProvider(DefaultMetricsCollectorProvider.class).in(Singleton.class);
    }

    protected void configureTransformationTraceService() {
        bind(TransformationTraceService.class).toProvider(TransformationTraceServiceProvider.class).in(Singleton.class);
    }

    protected void configureInstanceCollector() {
        bind(InstanceCollector.class).toProvider(RdbmsInstanceCollectorProvider.class).in(Singleton.class);
    }

    protected void configureDAO() {
        bind(DAO.class).toProvider(RdbmsDAOProvider.class).in(Singleton.class);
    }

    protected void configureActorResolver() {
        bind(ActorResolver.class).toProvider(DefaultActorResolverProvider.class).in(Singleton.class);
    }

    protected void configureDispatcherFunctionProvider() {
        bind(DispatcherFunctionProvider.class).toProvider(DispatcherFunctionProviderProvider.class).in(Singleton.class);
    }

    protected void configureOperationCallInterceptorProvider() {
        bind(OperationCallInterceptorProvider.class).toProvider(OperationCallInterceptorProviderProvider.class).in(Singleton.class);
    }

    protected void configureDispatcher() {
        bind(Dispatcher.class).toProvider(DefaultDispatcherProvider.class).asEagerSingleton();
    }

    protected void configureValidatorProvider() {
        bind(ValidatorProvider.class).toProvider(ValidatorProviderProvider.class).asEagerSingleton();
    }

    protected void configurePayloadValidator() {
        bind(PayloadValidator.class).toProvider(DefaultPayloadValidatorProvider.class).asEagerSingleton();
    }

    protected void configureExport() {
        bind(Export.class).to(UnsupportedExportImpl.class);
    }

    protected void configurePlatformTransactionManager() {
        bind(PlatformTransactionManager.class).toProvider(PlatformTransactionManagerProvider.class).in(Singleton.class);
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
        configureDispatcherFunctionProvider();
        configureOperationCallInterceptorProvider();
        configureDispatcher();
        configureValidatorProvider();
        configurePayloadValidator();
        configureExport();
        //configurePlatformTransactionManager();
    }
}
