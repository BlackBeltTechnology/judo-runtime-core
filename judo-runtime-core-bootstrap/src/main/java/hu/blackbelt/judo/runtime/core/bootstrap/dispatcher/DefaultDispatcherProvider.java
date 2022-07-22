package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

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

import javax.annotation.Nullable;
import javax.transaction.TransactionManager;

@SuppressWarnings("rawtypes")
public class DefaultDispatcherProvider implements Provider<Dispatcher> {

    public static final String DISPATCHER_METRICS_RETURNED = "dispatcherMetricsReturned";
    public static final String DISPATCHER_ENABLE_DEFAULT_VALIDATION = "dispatcherEnableDefaultValidation";
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
    TransactionManager transactionManager;

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
    @Named(DISPATCHER_ENABLE_DEFAULT_VALIDATION)
    @Nullable
    Boolean enableDefaultValidation;

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
                .enableDefaultValidation(enableDefaultValidation)
                .trimString(trimString)
                .caseInsensitiveLike(caseInsensitiveLike)
                .build();
    }
}
