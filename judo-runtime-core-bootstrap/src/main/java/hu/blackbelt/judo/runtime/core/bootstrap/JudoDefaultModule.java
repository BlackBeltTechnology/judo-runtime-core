package hu.blackbelt.judo.runtime.core.bootstrap;

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
import com.google.inject.name.Names;
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
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.runtime.core.validator.ValidatorProvider;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import hu.blackbelt.mapper.impl.DefaultCoercer;
import lombok.Builder;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.function.Consumer;

import static hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.RdbmsDAOProvider.RDBMS_DAO_MARK_SELECTED_RANGE_ITEMS;
import static hu.blackbelt.judo.runtime.core.bootstrap.dao.rdbms.RdbmsDAOProvider.RDBMS_DAO_OPTIMISTIC_LOCK_ENABLED;
import static hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.DefaultDispatcherProvider.*;
import static hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.DefaultIdentifierSignerProvider.IDENTIFIER_SIGNER_SECRET;
import static hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.DefaultMetricsCollectorProvider.*;
import static hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.DefaultPayloadValidatorProvider.PAYLOAD_VALIDATOR_REQUIRED_STRING_VALIDATOR_OPTION;
import static hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.ThreadContextProvider.THREAD_CONTEXT_DEBUG_THREAD_FORK;
import static hu.blackbelt.judo.runtime.core.bootstrap.dispatcher.ThreadContextProvider.THREAD_CONTEXT_INHERITABLE_CONTEXT;

public class JudoDefaultModule extends AbstractModule {

    public static final String ACTOR_RESOLVER_CHECK_MAPPED_ACTORS = "actorResolverCheckMappedActors";

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
}
