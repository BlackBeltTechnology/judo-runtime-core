package hu.blackbelt.judo.runtime.core.bootstrap.dispatcher;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AccessManager;
import hu.blackbelt.judo.runtime.core.bootstrap.JudoModelSpecification;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultDispatcher;
import hu.blackbelt.judo.runtime.core.dispatcher.DispatcherFunctionProvider;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;
import hu.blackbelt.judo.runtime.core.dispatcher.validators.Validator;
import hu.blackbelt.judo.runtime.core.security.OpenIdConfigurationProvider;
import hu.blackbelt.osgi.filestore.security.api.TokenIssuer;
import hu.blackbelt.osgi.filestore.security.api.TokenValidator;
import lombok.NonNull;

import javax.transaction.TransactionManager;

@SuppressWarnings("rawtypes")
public class DefaultDispatcherProvider implements Provider<Dispatcher> {

    public static final String DISPATCHER_METRICS_RETURNED = "dispatcherMetricsReturned";
    public static final String DISPATCHER_ENABLE_DEFAULT_VALIDATION = "dispatcherEnableDefaultValidation";
    public static final String DISPATCHER_TRIM_STRING = "dispatcherTrimString";
    public static final String DISPATCHER_CASE_INSENSITIVE_LIKE = "dispatcherCaseInsensitiveLike";
    public static final String DISPATCHER_REQUIRED_STRING_VALIDATOR_OPTION = "dispatcherRequiredStringValidatorOption";

    @Inject
    JudoModelSpecification models;

    @Inject
    DAO dao;

    @Inject
    IdentifierProvider identifierProvider;

    @Inject
    DispatcherFunctionProvider dispatcherFunctionProvider;

    @Inject(optional = true)
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

    @Inject(optional = true)
    OpenIdConfigurationProvider openIdConfigurationProvider;

    @Inject(optional = true)
    TokenIssuer filestoreTokenIssuer;

    @Inject(optional = true)
    TokenValidator filestoreTokenValidator;

    @Inject(optional = true)
    @Named(DISPATCHER_METRICS_RETURNED)
    Boolean metricsReturned;

    @Inject(optional = true)
    @Named(DISPATCHER_ENABLE_DEFAULT_VALIDATION)
    Boolean enableDefaultValidation;

    @Inject(optional = true)
    @Named(DISPATCHER_TRIM_STRING)
    Boolean trimString;

    @Inject(optional = true)
    @Named(DISPATCHER_CASE_INSENSITIVE_LIKE)
    Boolean caseInsensitiveLike;

    @Inject(optional = true)
    @Named(DISPATCHER_REQUIRED_STRING_VALIDATOR_OPTION)
    String requiredStringValidatorOption;

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
                .metricsCollector(metricsCollector)
                .openIdConfigurationProvider(openIdConfigurationProvider)
                .filestoreTokenValidator(filestoreTokenValidator)
                .filestoreTokenIssuer(filestoreTokenIssuer)
                .metricsReturned(metricsReturned)
                .enableDefaultValidation(enableDefaultValidation)
                .trimString(trimString)
                .caseInsensitiveLike(caseInsensitiveLike)
                .requiredStringValidatorOption(requiredStringValidatorOption)
                .build();
    }
}
