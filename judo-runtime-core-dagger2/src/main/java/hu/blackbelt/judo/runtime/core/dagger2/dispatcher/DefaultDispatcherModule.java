package hu.blackbelt.judo.runtime.core.dagger2.dispatcher;

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

import dagger.Module;
import dagger.Provides;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.PayloadValidator;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AccessManager;
import hu.blackbelt.judo.runtime.core.dagger2.JudoApplicationScope;
import hu.blackbelt.judo.runtime.core.dagger2.ModelHolder;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultDispatcher;
import hu.blackbelt.judo.runtime.core.dispatcher.DispatcherFunctionProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.OperationCallInterceptorProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;
import hu.blackbelt.judo.runtime.core.security.OpenIdConfigurationProvider;
import hu.blackbelt.judo.runtime.core.validator.ValidatorProvider;
import hu.blackbelt.osgi.filestore.security.api.TokenIssuer;
import hu.blackbelt.osgi.filestore.security.api.TokenValidator;
import org.springframework.transaction.PlatformTransactionManager;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import static java.util.Objects.requireNonNullElse;

@SuppressWarnings("rawtypes")
@Module
public class DefaultDispatcherModule {

    public static final String DISPATCHER_METRICS_RETURNED = "dispatcherMetricsReturned";
    public static final String DISPATCHER_ENABLE_VALIDATION = "dispatcherEnableDefaultValidation";
    public static final String DISPATCHER_TRIM_STRING = "dispatcherTrimString";
    public static final String DISPATCHER_CASE_INSENSITIVE_LIKE = "dispatcherCaseInsensitiveLike";


    @SuppressWarnings("unchecked")
    @JudoApplicationScope
    @Provides
    public Dispatcher providesDispatcher(
            ModelHolder models,
            DAO dao,
            IdentifierProvider identifierProvider,
            DispatcherFunctionProvider dispatcherFunctionProvider,
            OperationCallInterceptorProvider operationCallInterceptorProvider,
            @Nullable PlatformTransactionManager transactionManager,
            DataTypeManager dataTypeManager,
            IdentifierSigner identifierSigner,
            AccessManager accessManager,
            ActorResolver actorResolver,
            Context context,
            MetricsCollector metricsCollector,
            PayloadValidator payloadValidator,
            ValidatorProvider validatorProvider,
            @Nullable OpenIdConfigurationProvider openIdConfigurationProvider,
            @Nullable TokenIssuer filestoreTokenIssuer,
            @Nullable TokenValidator filestoreTokenValidator,
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
                .transactionManager(transactionManager)
                .dataTypeManager(dataTypeManager)
                .identifierSigner(identifierSigner)
                .accessManager(accessManager)
                .actorResolver(actorResolver)
                .context(context)
                .validatorProvider(validatorProvider)
                .payloadValidator(payloadValidator)
                .metricsCollector(metricsCollector)
                .openIdConfigurationProvider(openIdConfigurationProvider)
                .filestoreTokenValidator(filestoreTokenValidator)
                .filestoreTokenIssuer(filestoreTokenIssuer)
                .metricsReturned(metricsReturned)
                .enableValidation(enableValidation)
                .trimString(trimString)
                .caseInsensitiveLike(requireNonNullElse(caseInsensitiveLike, false))
                .build();
    }
}
