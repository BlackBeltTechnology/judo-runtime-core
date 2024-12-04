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
import lombok.Builder;
import org.eclipse.emf.ecore.EReference;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class JudoDefaultModule extends AbstractModule {

    public final Object injectModulesTo;
    public final JudoModelLoader judoModelLoader;
    public final Boolean bindModelHolder;
    public final Map<EReference, CustomJoinDefinition> queryFactoryCustomJoinDefinitions;
    public final Boolean rdbmsDaoOptimisticLockEnabled;
    public final Boolean rdbmsDaoMarkSelectedRangeItems;
    public final Integer rdbmsDaoChunkSize;
    public final Boolean actorResolverCheckMappedActors;
    public final Boolean dispatcherMetricsReturned;
    public final Boolean dispatcherEnableDefaultValidation;
    public final Boolean dispatcherTrimString;
    public final Boolean dispatcherCaseInsensitiveLike;
    public final String identifierSignerSecret;
    public final Consumer metricsCollectorConsumer;
    public final Boolean metricsCollectorEnabled;
    public final Boolean metricsCollectorVerbose;
    public final String payloadValidatorRequiredStringValidatorOption;
    public final Boolean threadContextDebugThreadFork;
    public final Boolean threadContextInheritableContext;
    public final Long rdbmsSequenceStart;
    public final Long rdbmsSequenceIncrement;
    public final Boolean rdbmsSequenceCreateIfNotExists;

    public static class JudoDefaultModuleBuilder {
        Object injectModulesTo = false;
        JudoModelLoader judoModelLoader = null;
        Boolean bindModelHolder = true;
        Map<EReference, CustomJoinDefinition> queryFactoryCustomJoinDefinitions = new ConcurrentHashMap<>();
        Boolean rdbmsDaoOptimisticLockEnabled = true;
        Boolean rdbmsDaoMarkSelectedRangeItems = false;
        Integer rdbmsDaoChunkSize = 1000;
        Boolean actorResolverCheckMappedActors = false;
        Boolean dispatcherMetricsReturned = false;
        Boolean dispatcherEnableDefaultValidation = true;
        Boolean dispatcherCaseInsensitiveLike = false;
        Boolean dispatcherTrimString = false;

        String identifierSignerSecret;
        Consumer metricsCollectorConsumer = (m) -> {};
        Boolean metricsCollectorEnabled = false;
        Boolean metricsCollectorVerbose = false;
        String payloadValidatorRequiredStringValidatorOption = DefaultPayloadValidatorProvider.ACCEPT_NON_EMPTY;
        Boolean threadContextDebugThreadFork = false;
        Boolean threadContextInheritableContext = true;
        Long rdbmsSequenceStart = 1L;
        Long rdbmsSequenceIncrement = 1L;
        Boolean rdbmsSequenceCreateIfNotExists = true;
    }

    @Builder
    public JudoDefaultModule(Object injectModulesTo,
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
        this.injectModulesTo = injectModulesTo;
        this.judoModelLoader = judoModelLoader;
        this.bindModelHolder = bindModelHolder;
        this.queryFactoryCustomJoinDefinitions = queryFactoryCustomJoinDefinitions;
        this.rdbmsDaoOptimisticLockEnabled = rdbmsDaoOptimisticLockEnabled;
        this.rdbmsDaoMarkSelectedRangeItems = rdbmsDaoMarkSelectedRangeItems;
        this.rdbmsDaoChunkSize = rdbmsDaoChunkSize;
        this.actorResolverCheckMappedActors = actorResolverCheckMappedActors;
        this.dispatcherMetricsReturned = dispatcherMetricsReturned;
        this.dispatcherEnableDefaultValidation = dispatcherEnableDefaultValidation;
        this.dispatcherTrimString = dispatcherTrimString;
        this.dispatcherCaseInsensitiveLike = dispatcherCaseInsensitiveLike;
        this.identifierSignerSecret = Objects.requireNonNullElseGet(identifierSignerSecret, () -> generateNewSecret());
        this.metricsCollectorConsumer = metricsCollectorConsumer;
        this.metricsCollectorEnabled = metricsCollectorEnabled;
        this.metricsCollectorVerbose = metricsCollectorVerbose;
        this.payloadValidatorRequiredStringValidatorOption = payloadValidatorRequiredStringValidatorOption;
        this.threadContextDebugThreadFork = threadContextDebugThreadFork;
        this.threadContextInheritableContext = threadContextInheritableContext;
        this.rdbmsSequenceStart = rdbmsSequenceStart;
        this.rdbmsSequenceIncrement = rdbmsSequenceIncrement;
        this.rdbmsSequenceCreateIfNotExists = rdbmsSequenceCreateIfNotExists;

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
        bind(AsmModel.class).toInstance(judoModelLoader.getAsmModel());
        bind(RdbmsModel.class).toInstance(judoModelLoader.getRdbmsModel());
        bind(MeasureModel.class).toInstance(judoModelLoader.getMeasureModel());
        bind(LiquibaseModel.class).toInstance(judoModelLoader.getLiquibaseModel());
        bind(ExpressionModel.class).toInstance(judoModelLoader.getExpressionModel());

        // Model
        if (bindModelHolder) {
            bind(JudoModelLoader.class).toInstance(judoModelLoader);
        }
    }

    protected void configureOptions() {
        bind(Map.class).annotatedWith(JudoModuleConfiguration.QueryFactoryCustomJoinDefinitions.class).toInstance(queryFactoryCustomJoinDefinitions);
        bind(Boolean.class).annotatedWith(JudoModuleConfiguration.RdbmsDaoOptimisticLockEnabled.class).toInstance(rdbmsDaoOptimisticLockEnabled);
        bind(Boolean.class).annotatedWith(JudoModuleConfiguration.RdbmsDaoMarkSelectedRangeItems.class).toInstance(rdbmsDaoMarkSelectedRangeItems);
        bind(Integer.class).annotatedWith(JudoModuleConfiguration.RdbmsDaoChunkSize.class).toInstance(rdbmsDaoChunkSize);
        bind(Boolean.class).annotatedWith(JudoModuleConfiguration.ActorResolverCheckMappedActors.class).toInstance(actorResolverCheckMappedActors);
        bind(Boolean.class).annotatedWith(JudoModuleConfiguration.DispatcherMetricsReturned.class).toInstance(dispatcherMetricsReturned);
        bind(Boolean.class).annotatedWith(JudoModuleConfiguration.DispatcherEnableDefaultValidation.class).toInstance(dispatcherEnableDefaultValidation);
        bind(Boolean.class).annotatedWith(JudoModuleConfiguration.DispatcherTrimString.class).toInstance(dispatcherTrimString);
        bind(Boolean.class).annotatedWith(JudoModuleConfiguration.DispatcherCaseInsensitiveLike.class).toInstance(dispatcherCaseInsensitiveLike);
        bind(String.class).annotatedWith(JudoModuleConfiguration.IdentifierSignerSecret.class).toInstance(identifierSignerSecret);
        bind(Consumer.class).annotatedWith(JudoModuleConfiguration.MetricsCollectorConsumer.class).toInstance(metricsCollectorConsumer);
        bind(Boolean.class).annotatedWith(JudoModuleConfiguration.MetricsCollectorEnabled.class).toInstance(metricsCollectorEnabled);
        bind(Boolean.class).annotatedWith(JudoModuleConfiguration.MetricsCollectorVerbose.class).toInstance(metricsCollectorVerbose);
        bind(String.class).annotatedWith(JudoModuleConfiguration.PayloadValidatorRequiredStringValidatorOption.class).toInstance(payloadValidatorRequiredStringValidatorOption);
        bind(Boolean.class).annotatedWith(JudoModuleConfiguration.ThreadContextDebugThreadFork.class).toInstance(threadContextDebugThreadFork);
        bind(Boolean.class).annotatedWith(JudoModuleConfiguration.ThreadContextInheritableContext.class).toInstance(threadContextInheritableContext);
        bind(Long.class).annotatedWith(JudoModuleConfiguration.RdbmsSequenceStart.class).toInstance(rdbmsSequenceStart);
        bind(Long.class).annotatedWith(JudoModuleConfiguration.RdbmsSequenceIncrement.class).toInstance(rdbmsSequenceIncrement);
        bind(Boolean.class).annotatedWith(JudoModuleConfiguration.RdbmsSequenceCreateIfNotExists.class).toInstance(rdbmsSequenceCreateIfNotExists);
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

    protected void configure() {
        if (injectModulesTo != null) {
            requestInjection(injectModulesTo);
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
    }
}
