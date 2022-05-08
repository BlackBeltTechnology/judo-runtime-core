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

public class DefaultDispatcherProvider implements Provider<Dispatcher> {

    public static final String DISPATCHER_METRICS_RETURNED = "dispatcherMetricsReturned";
    public static final String DISPATCHER_ENABLE_DEFAULT_VALIDATION = "dispatcherEnableDefaultValidation";
    public static final String DISPATCHER_TRIM_STRING = "dispatcherTrimString";
    public static final String DISPATCHER_CASE_INSENSITIVE_LIKE = "dispatcherCaseInsensitiveLike";
    public static final String DISPATCHER_REQUIRED_STRING_VALIDATOR_OPTION = "dispatcherRequiredStringValidatorOption";
    AsmModel asmModel;
    ExpressionModel expressionModel;
    DAO dao;
    IdentifierProvider identifierProvider;
    DispatcherFunctionProvider dispatcherFunctionProvider;
    TransactionManager transactionManager;
    DataTypeManager dataTypeManager;
    IdentifierSigner identifierSigner;
    AccessManager accessManager;
    ActorResolver actorResolver;
    OpenIdConfigurationProvider openIdConfigurationProvider;
    Context context;
    MetricsCollector metricsCollector;
    TokenIssuer filestoreTokenIssuer;
    TokenValidator filestoreTokenValidator;
    Validator rangeValidator;
    Boolean metricsReturned;
    Boolean enableDefaultValidation;
    Boolean trimString;
    Boolean caseInsensitiveLike;
    String requiredStringValidatorOption;

    @Inject
    public DefaultDispatcherProvider(AsmModel asmModel,
                                     ExpressionModel expressionModel,
                                     DAO dao,
                                     IdentifierProvider identifierProvider,
                                     DispatcherFunctionProvider dispatcherFunctionProvider,
                                     TransactionManager transactionManager,
                                     DataTypeManager dataTypeManager,
                                     IdentifierSigner identifierSigner,
                                     AccessManager accessManager,
                                     ActorResolver actorResolver,
                                     OpenIdConfigurationProvider openIdConfigurationProvider,
                                     Context context,
                                     MetricsCollector metricsCollector,
                                     TokenIssuer filestoreTokenIssuer,
                                     TokenValidator filestoreTokenValidator,
                                     Validator rangeValidator,
                                     @Named(DISPATCHER_METRICS_RETURNED) Boolean metricsReturned,
                                     @Named(DISPATCHER_ENABLE_DEFAULT_VALIDATION) Boolean enableDefaultValidation,
                                     @Named(DISPATCHER_TRIM_STRING) Boolean trimString,
                                     @Named(DISPATCHER_CASE_INSENSITIVE_LIKE) Boolean caseInsensitiveLike,
                                     @Named(DISPATCHER_REQUIRED_STRING_VALIDATOR_OPTION) String requiredStringValidatorOption) {
        this.asmModel = asmModel;
        this.expressionModel = expressionModel;
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.dispatcherFunctionProvider = dispatcherFunctionProvider;
        this.transactionManager = transactionManager;
        this.dataTypeManager = dataTypeManager;
        this.identifierSigner = identifierSigner;
        this.accessManager = accessManager;
        this.actorResolver = actorResolver;
        this.openIdConfigurationProvider = openIdConfigurationProvider;
        this.context = context;
        this.metricsCollector = metricsCollector;
        this.filestoreTokenIssuer = filestoreTokenIssuer;
        this.filestoreTokenValidator = filestoreTokenValidator;
        this.rangeValidator = rangeValidator;
        this.metricsReturned = metricsReturned;
        this.enableDefaultValidation = enableDefaultValidation;
        this.trimString = trimString;
        this.caseInsensitiveLike = caseInsensitiveLike;
        this.requiredStringValidatorOption = requiredStringValidatorOption;
    }


    @Override
    public Dispatcher get() {
        return DefaultDispatcher.builder()
                .asmModel(asmModel)
                .expressionModel(expressionModel)
                .dao(dao)
                .identifierProvider(identifierProvider)
                .dispatcherFunctionProvider(dispatcherFunctionProvider)
                .transactionManager(transactionManager)
                .dataTypeManager(dataTypeManager)
                .identifierSigner(identifierSigner)
                .accessManager(accessManager)
                .actorResolver(actorResolver)
                .openIdConfigurationProvider(openIdConfigurationProvider)
                .context(context)
                .metricsCollector(metricsCollector)
                .filestoreTokenValidator(filestoreTokenValidator)
                .filestoreTokenIssuer(filestoreTokenIssuer)
                .rangeValidator(rangeValidator)
                .metricsReturned(metricsReturned)
                .enableDefaultValidation(enableDefaultValidation)
                .trimString(trimString)
                .caseInsensitiveLike(caseInsensitiveLike)
                .requiredStringValidatorOption(requiredStringValidatorOption)
                .build();
    }
}
