package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

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

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.PayloadValidator;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AccessManager;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelLoader;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultDispatcher;
import hu.blackbelt.judo.runtime.core.dispatcher.DispatcherFunctionProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;
import hu.blackbelt.judo.runtime.core.security.OpenIdConfigurationProvider;
import hu.blackbelt.judo.runtime.core.validator.ValidatorProvider;
import hu.blackbelt.osgi.filestore.security.api.TokenIssuer;
import hu.blackbelt.osgi.filestore.security.api.TokenValidator;
import org.springframework.transaction.PlatformTransactionManager;

import javax.annotation.Nullable;

@SuppressWarnings("rawtypes")
public class DefaultDispatcherProvider implements Provider<Dispatcher> {

    public static final String DISPATCHER_METRICS_RETURNED = "dispatcherMetricsReturned";
    public static final String DISPATCHER_ENABLE_VALIDATION = "dispatcherEnableDefaultValidation";
    public static final String DISPATCHER_TRIM_STRING = "dispatcherTrimString";
    public static final String DISPATCHER_CASE_INSENSITIVE_LIKE = "dispatcherCaseInsensitiveLike";

    @Inject
    JudoModelLoader models;

    @Inject
    DAO dao;

    @Inject
    IdentifierProvider identifierProvider;

    @Inject
    DispatcherFunctionProvider dispatcherFunctionProvider;

    @Inject(optional = true)
    @Nullable
    PlatformTransactionManager transactionManager;

    @Inject
    DataTypeManager dataTypeManager;

    @Inject
    IdentifierSigner identifierSigner;

    @Inject
    AccessManager accessManager;

    @Inject
    ActorResolver actorResolver;

    @Inject
    Context context;

    @Inject
    MetricsCollector metricsCollector;

    @Inject
    PayloadValidator payloadValidator;

    @Inject
    ValidatorProvider validatorProvider;

    @Inject(optional = true)
    @Nullable
    OpenIdConfigurationProvider openIdConfigurationProvider;

    @Inject(optional = true)
    @Nullable
    TokenIssuer filestoreTokenIssuer;

    @Inject(optional = true)
    @Nullable
    TokenValidator filestoreTokenValidator;

    @Inject(optional = true)
    @Named(DISPATCHER_METRICS_RETURNED)
    @Nullable
    Boolean metricsReturned;

    @Inject(optional = true)
    @Named(DISPATCHER_ENABLE_VALIDATION)
    @Nullable
    Boolean enableValidation;

    @Inject(optional = true)
    @Named(DISPATCHER_TRIM_STRING)
    @Nullable
    Boolean trimString;

    @Inject(optional = true)
    @Named(DISPATCHER_CASE_INSENSITIVE_LIKE)
    @Nullable
    Boolean caseInsensitiveLike;

    @Override
    @SuppressWarnings("unchecked")
    public Dispatcher get() {
        return DefaultDispatcher.builder()
                .asmModel(models.getAsmModel())
                .expressionModel(models.getExpressionModel())
                .dao(dao)
                .identifierProvider(identifierProvider)
                .dispatcherFunctionProvider(dispatcherFunctionProvider)
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
                .caseInsensitiveLike(caseInsensitiveLike)
                .build();
    }
}
