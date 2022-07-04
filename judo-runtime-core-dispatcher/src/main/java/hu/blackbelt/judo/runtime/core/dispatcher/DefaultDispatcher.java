package hu.blackbelt.judo.runtime.core.dispatcher;

import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.BusinessException;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.runtime.ExpressionModel;
import hu.blackbelt.judo.runtime.core.exception.*;
import hu.blackbelt.judo.runtime.core.accessmanager.api.AccessManager;
import hu.blackbelt.judo.runtime.core.accessmanager.api.SignedIdentifier;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCancelToken;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.dispatcher.behaviours.*;
import hu.blackbelt.judo.runtime.core.dispatcher.converters.FileTypeFormatter;
import hu.blackbelt.judo.runtime.core.dispatcher.security.ActorResolver;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;
import hu.blackbelt.judo.runtime.core.dispatcher.validators.*;
import hu.blackbelt.judo.runtime.core.security.OpenIdConfigurationProvider;
import hu.blackbelt.osgi.filestore.security.api.*;
import hu.blackbelt.osgi.filestore.security.api.exceptions.InvalidTokenException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.*;
import org.slf4j.MDC;

import javax.transaction.TransactionManager;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Optional.ofNullable;

@Slf4j
public class DefaultDispatcher<ID> implements Dispatcher {

    public static final String UPDATEABLE_KEY = "__updateable";
    public static final String DELETEABLE_KEY = "__deleteable";
    public static final String IMMUTABLE_KEY = "__immutable";
    public static final String SELECTED_ITEM_KEY = "__selected";
    public static final String REFERENCE_ID_KEY = "__referenceId";
    public static final String VERSION_KEY = "__version";
    public static final String SDK = "sdk";
    public static final String SCRIPT = "script";
    public static final String BEHAVIOUR = "behaviour";

    public static final String FAULT = "_fault";
    public static final String FAULT_TYPE = "_type";
    public static final String FAULT_ERROR_CODE = "_errorCode";
    public static final String FAULT_CAUSE = "_cause";

    private static final String STATEFUL = "STATEFUL";

    private static final String METRICS_DISPATCHER = "dispatcher";
    private static final String METRICS_SCRIPT_CALL = "call-script";
    private static final String METRICS_SDK_CALL = "call-sdk";
    private static final String METRICS_JUDO_CALL = "call-judo";

    private static final Collection<Validator> DEFAULT_VALIDATORS = Arrays.asList(new MaxLengthValidator(), new MinLengthValidator(), new PrecisionValidator(), new PatternValidator());

    private final AsmModel asmModel;

    private final ExpressionModel expressionModel;

    private final DAO<ID> dao;

    private final IdentifierProvider<ID> identifierProvider;

    private final DispatcherFunctionProvider dispatcherFunctionProvider;

    private final DataTypeManager dataTypeManager;

    private final IdentifierSigner identifierSigner;

    private final ActorResolver actorResolver;

    private final TransactionManager transactionManager;

    private final AccessManager accessManager;

    private final  OpenIdConfigurationProvider openIdConfigurationProvider;

    private final Context context;

    private final MetricsCollector metricsCollector;

    private final TokenIssuer filestoreTokenIssuer;

    private final TokenValidator filestoreTokenValidator;

    private final Validator rangeValidator;

    private final Boolean metricsReturned;

    private final Boolean trimString;

    private final Boolean caseInsensitiveLike;

    private final String requiredStringValidatorOption;

    private final Collection<Validator> validators = new ArrayList<>();

    private Set<BehaviourCall<ID>> behaviourCalls;

    @SuppressWarnings("unchecked")
	private void setupBehaviourCalls(DAO<ID> dao, IdentifierProvider<ID> identifierProvider, AsmUtils asmUtils) {
        behaviourCalls = ImmutableSet.<BehaviourCall<ID>>builder()
                .add(
                        new ListCall<ID>(context, dao, identifierProvider, asmUtils, transactionManager, dataTypeManager.getCoercer(), actorResolver, caseInsensitiveLike),
                        new CreateInstanceCall<ID>(dao, identifierProvider, asmUtils, transactionManager),
                        new ValidateCreateCall<ID>(context, dao, identifierProvider, asmUtils, transactionManager),
                        new RefreshCall<ID>(context, dao, identifierProvider, asmUtils, transactionManager, dataTypeManager.getCoercer(), caseInsensitiveLike),
                        new UpdateInstanceCall<ID>(dao, identifierProvider, asmUtils, transactionManager, dataTypeManager.getCoercer()),
                        new ValidateUpdateCall<ID>(context, dao, identifierProvider, asmUtils, transactionManager, dataTypeManager.getCoercer()),
                        new DeleteInstanceCall<ID>(dao, identifierProvider, asmUtils, transactionManager),
                        new SetReferenceCall<ID>(dao, identifierProvider, asmUtils, transactionManager),
                        new UnsetReferenceCall<ID>(dao, identifierProvider, asmUtils, transactionManager),
                        new AddReferenceCall<ID>(dao, identifierProvider, asmUtils, transactionManager),
                        new RemoveReferenceCall<ID>(dao, identifierProvider, asmUtils, transactionManager),
                        new GetReferenceRangeCall<ID>(context, dao, identifierProvider, asmUtils, expressionModel, transactionManager, dataTypeManager.getCoercer(), caseInsensitiveLike),
                        new GetInputRangeCall<ID>(context, dao, identifierProvider, asmUtils, expressionModel, transactionManager, dataTypeManager.getCoercer(), caseInsensitiveLike),
                        new GetPrincipalCall<ID>(dao, identifierProvider, asmUtils, actorResolver),
                        new GetTemplateCall<ID>(dao, asmUtils),
                        new GetMetadataCall<ID>(asmUtils, () -> openIdConfigurationProvider),
                        new GetUploadTokenCall<ID>(asmUtils, filestoreTokenIssuer)
                )
                .build();
    }

    @Builder
    public DefaultDispatcher(
            @NonNull AsmModel asmModel,
            @NonNull ExpressionModel expressionModel,
            @NonNull DAO<ID> dao,
            @NonNull IdentifierProvider<ID> identifierProvider,
            @NonNull DispatcherFunctionProvider dispatcherFunctionProvider,
            @NonNull DataTypeManager dataTypeManager,
            @NonNull IdentifierSigner identifierSigner,
            @NonNull ActorResolver actorResolver,
            @NonNull Context context,
            @NonNull MetricsCollector metricsCollector,
            OpenIdConfigurationProvider openIdConfigurationProvider,
            TokenIssuer filestoreTokenIssuer,
            TokenValidator filestoreTokenValidator,
            AccessManager accessManager,
            TransactionManager transactionManager,
            Boolean metricsReturned,
            Boolean enableDefaultValidation,
            Boolean trimString,
            Boolean caseInsensitiveLike,
            String requiredStringValidatorOption
        ) {
        this.asmModel = asmModel;
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.dispatcherFunctionProvider = dispatcherFunctionProvider;
        this.dataTypeManager = dataTypeManager;
        this.identifierSigner = identifierSigner;

        if (accessManager == null) {
            this.accessManager = (operation, signedIdentifier, exchange) -> {};
        } else {
            this.accessManager = accessManager;
        }

        this.actorResolver = actorResolver;
        this.transactionManager = transactionManager;
        this.expressionModel = expressionModel;
        this.context = context;
        this.metricsCollector = metricsCollector;

        if (enableDefaultValidation == null || enableDefaultValidation) {
            validators.addAll(DEFAULT_VALIDATORS);
            rangeValidator = new RangeValidator<>(dao, identifierProvider, context, transactionManager);
            validators.add(rangeValidator);
        } else {
            rangeValidator = null;
        }

        if (metricsReturned != null) {
            this.metricsReturned = metricsReturned;
        } else {
            this.metricsReturned = true;}

        if (trimString != null) {
            this.trimString = trimString;
        } else {
            this.trimString = false;
        }

        if (caseInsensitiveLike != null) {
            this.caseInsensitiveLike = caseInsensitiveLike;
        } else {
            this.caseInsensitiveLike = false;
        }

        if (requiredStringValidatorOption != null) {
            this.requiredStringValidatorOption = requiredStringValidatorOption;
        } else {
            this.requiredStringValidatorOption = "ACCEPT_NON_EMPTY";
        }

        this.openIdConfigurationProvider = openIdConfigurationProvider;

        if (filestoreTokenIssuer == null) {
            this.filestoreTokenIssuer = new TokenIssuer() {
                @Override
                public String createUploadToken(Token<UploadClaim> token) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public String createDownloadToken(Token<DownloadClaim> token) {
                    throw new UnsupportedOperationException();
                }
            };
        } else {
            this.filestoreTokenIssuer = filestoreTokenIssuer;
        }

        if (filestoreTokenValidator == null) {
            this.filestoreTokenValidator =  new TokenValidator() {
                @Override
                public Token<UploadClaim> parseUploadToken(String tokenString) throws InvalidTokenException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Token<DownloadClaim> parseDownloadToken(String tokenString) throws InvalidTokenException {
                    throw new UnsupportedOperationException();
                }
            };
        } else {
            this.filestoreTokenValidator = filestoreTokenValidator;
        }

        asmUtils = new AsmUtils(asmModel.getResourceSet());
        setupBehaviourCalls(dao, identifierProvider, asmUtils);
        registerDataTypes();
    }

    private AsmUtils asmUtils;

    /*
    @Activate
    protected void activate(Config config) {
        asmUtils = new AsmUtils(asmModel.getResourceSet());
        caseInsensitiveLike = config.caseInsensitiveLike();
        setupBehaviourCalls(dao, identifierProvider, asmUtils);
        if (config.enableDefaultValidation()) {
            validators.addAll(DEFAULT_VALIDATORS);
            rangeValidator = new RangeValidator<>(dao, identifierProvider, context, transactionManager);
            validators.add(rangeValidator);
        }
        metricsReturned = config.metricsReturned();
        trimString = config.trimString();
        requiredStringValidatorOption = config.requiredStringValidatorOption();
        registerDataTypes();
    }

    @Deactivate
    protected void deactivate() {
        unregisterDataTypes();
        asmUtils = null;
        validators.removeAll(DEFAULT_VALIDATORS);
        if (rangeValidator != null) {
            validators.remove(rangeValidator);
            rangeValidator = null;
        }
    }
    */
    private void registerDataTypes() {
        getAsmUtils().all(EDataType.class)
                .filter(t -> "byte[]".equals(t.getInstanceClassName()))
                .forEach(t -> dataTypeManager.registerCustomType(t, t.getInstanceClassName(), null, new FileTypeFormatter()));
    }

    @SuppressWarnings("unused")
	private void unregisterDataTypes() {
        getAsmUtils().all(EDataType.class)
                .filter(t -> "byte[]".equals(t.getInstanceClassName()))
                .forEach(t -> dataTypeManager.unregisterCustomType(t));
    }

    void addValidator(Validator validator) {
        validators.add(validator);
    }

    void removeValidator(Validator validator) {
        validators.remove(validator);
    }

    private Optional<SignedIdentifier> getIdForBoundOperation(final EClass mappedTransferObjectType, final Map<String, Object> exchange, final boolean exposed) {
        if (exposed) {
            if (exchange.get(IdentifierSigner.SIGNED_IDENTIFIER_KEY) == null) {
                log.info("Operation failed, missing " + IdentifierSigner.SIGNED_IDENTIFIER_KEY + " header of bound operation");
                throw new ValidationException(Collections.singleton(FeedbackItem.builder()
                        .code("MISSING_IDENTIFIER_OF_BOUND_OPERATION")
                        .level(FeedbackItem.Level.ERROR)
                        .build()));
            }

            try {
                return identifierSigner.extractSignedIdentifier(mappedTransferObjectType, exchange);
            } catch (ClientException ex) {
                throw ex;
            } catch (RuntimeException ex) {
                log.info("Invalid signature of bound instance: {}", ex.getMessage());
                if (log.isDebugEnabled()) {
                    log.debug("Extracting signature failed", ex);
                }
                throw new ValidationException(Collections.singleton(FeedbackItem.builder()
                        .code("INVALID_IDENTIFIER")
                        .level(FeedbackItem.Level.ERROR)
                        .build()));
            }
        } else {
            checkArgument(exchange.containsKey(identifierProvider.getName()), "Bound operation must have an identifier");
            @SuppressWarnings("unchecked")
			final ID id = (ID) exchange.get(identifierProvider.getName());
            final String entityType = (String) exchange.get(ENTITY_TYPE_MAP_KEY);
            return Optional.of(SignedIdentifier.builder()
                    .identifier(dataTypeManager.getCoercer().coerce(id, String.class))
                    .entityType(entityType)
                    .build());
        }
    }

    private EClass getContainerOfBoundOperation(EOperation operation) {
        final EObject containerOfOperation = operation.eContainer();
        checkState(containerOfOperation != null
                        && (containerOfOperation instanceof EClass)
                        && (getAsmUtils().isMappedTransferObjectType((EClass) containerOfOperation)),
                "Container of bound transfer operation must be a mapped transfer object type");
        return (EClass) containerOfOperation;
    }

    private Payload getTransferObjectAsBoundType(EClass mappedTransferObjectType, SignedIdentifier signedIdentifier) {
        final ID id = dataTypeManager.getCoercer().coerce(signedIdentifier.getIdentifier(), identifierProvider.getType());
        return dao.getByIdentifier(mappedTransferObjectType, id)
                .orElseThrow(() -> new NotFoundException(FeedbackItem.builder()
                        .code("BOUND_OPERATION_INSTANCE_NOT_FOUND")
                        .level(FeedbackItem.Level.ERROR)
                        .build()));
    }

    private void checkProducedByOfBoundOperationInstance(final SignedIdentifier signedIdentifier, final EOperation operation) {
        if (signedIdentifier.getProducedBy() == null) {
            throw new SecurityException("Producer of '" + signedIdentifier.getIdentifier() + "' is missing");
        }

        final ETypedElement producedBy = signedIdentifier.getProducedBy();
        if (!AsmUtils.equals(operation.eContainer(), producedBy.getEType()) &&
                !((producedBy.getEType() instanceof EClass) && ((EClass) producedBy.getEType()).getEAllSuperTypes().contains(operation.eContainer()))) {
            log.info("Mapped transfer object type of bound operation {} does not match type of signed identifier {}", AsmUtils.getOperationFQName(operation), AsmUtils.getClassifierFQName(producedBy.getEType()));
            throw new AccessDeniedException(FeedbackItem.builder()
                    .code("ACCESS_DENIED_INVALID_TYPE")
                    .level(FeedbackItem.Level.ERROR)
                    .build());
        }
    }

    private Set<BehaviourCall<ID>> getBehaviourCalls() {
        if (behaviourCalls == null) {
            setupBehaviourCalls(dao, identifierProvider, getAsmUtils());
        }
        return behaviourCalls;
    }

    private AsmUtils getAsmUtils() {
        if (asmUtils == null) {
            asmUtils = new AsmUtils(asmModel.getResourceSet());
        }
        return asmUtils;
    }

    private Collection<Validator> getValidators() {
        if (validators.size() == 0) {
            validators.addAll(DEFAULT_VALIDATORS);
            validators.add(new RangeValidator<>(dao, identifierProvider, context, transactionManager));
        }
        return validators;
    }

    @SuppressWarnings("unchecked")
	@Override
    public Map<String, Object> callOperation(final String operationFullyQualifiedName, final Map<String, Object> exchange) {
        MDC.put("operation", operationFullyQualifiedName);
        Payload result = null;

        final long startTs = System.nanoTime();


            final boolean exposed = Boolean.TRUE.equals(exchange.remove("__exposed"));
            final Boolean stateful = context.getAs(Boolean.class, STATEFUL);
            // TODO - do not cleanup context of operations called by dispatcher
            if (context != null && exposed) {
                context.removeAll();
            }

            try (MetricsCancelToken ct = metricsCollector.start(METRICS_DISPATCHER)) {
                actorResolver.authenticateActor(exchange);
                if (exchange.containsKey(ACTOR_KEY)) {
                    context.putIfAbsent(ACTOR_KEY, exchange.get(ACTOR_KEY));
                }

                final EOperation operation = getAsmUtils().all(EOperation.class)
                        .filter(op -> Objects.equals(AsmUtils.getOperationFQName(op), operationFullyQualifiedName))
                        .findAny()
                        .orElseThrow(() -> new UnsupportedOperationException("Operation not found: " + operationFullyQualifiedName));

                final boolean immutable = AsmUtils.annotatedAsTrue(operation, "immutable");

                // operation marked stateful if annotation is not present AND operation call is called by already running stateless operation
                final boolean stateless = AsmUtils.annotatedAsFalse(operation, "stateful");
                context.put(STATEFUL, !stateless && !Boolean.FALSE.equals(stateful));

                Optional<SignedIdentifier> id = Optional.empty();
                Optional<EClass> entityType = Optional.empty();
                EOperation implementation;
                Optional<EClass> mappedTransferObjectType = Optional.empty();

                if (AsmUtils.isBound(operation)) {
                    mappedTransferObjectType = Optional.of(getContainerOfBoundOperation(operation));

                    id = getIdForBoundOperation(mappedTransferObjectType.get(), exchange, exposed);
                    checkArgument(id.isPresent() && id.get().getIdentifier() != null, "Missing ID of bound operation");
                    if (exposed) {
                        if (Boolean.TRUE.equals(id.get().getImmutable())) {
                            throw new AccessDeniedException(FeedbackItem.builder()
                                    .code("BOUND_OPERATION_INSTANCE_IS_IMMUTABLE")
                                    .level(FeedbackItem.Level.ERROR)
                                    .build());
                        }
                        accessManager.authorizeOperation(operation, id.get(), exchange);
                        checkProducedByOfBoundOperationInstance(id.get(), operation);
                    }

                    if (id.get().getEntityType() != null) {
                        entityType = Optional.of(getAsmUtils().resolve(id.get().getEntityType()).filter(t -> t instanceof EClass).map(t -> (EClass) t)
                                .orElseThrow(() -> new IllegalArgumentException("Unable to resolve entity type")));
                        final Payload metadata = dao.getMetadata(mappedTransferObjectType.get(), dataTypeManager.getCoercer().coerce(id.get().getIdentifier(), identifierProvider.getType()))
                                .orElseThrow(() -> new NotFoundException(FeedbackItem.builder()
                                        .code("BOUND_OPERATION_INSTANCE_NOT_FOUND")
                                        .level(FeedbackItem.Level.ERROR)
                                        .build()));
                        checkState(Objects.equals(AsmUtils.getClassifierFQName(entityType.get()), metadata.get(Dispatcher.ENTITY_TYPE_MAP_KEY)), "Invalid entity type in signed identifier");
                    } else {
                        final Payload metadata = getTransferObjectAsBoundType(mappedTransferObjectType.get(), id.get());
                        final String entityTypeFQName = metadata.getAs(String.class, ENTITY_TYPE_MAP_KEY);
                        checkArgument(entityTypeFQName != null, "Entity type is unknown");

                        entityType = Optional.of(getAsmUtils().resolve(entityTypeFQName).filter(t -> t instanceof EClass).map(t -> (EClass) t)
                                .orElseThrow(() -> new IllegalArgumentException("Unable to resolve entity type")));
                    }

                    final String boundOperationName = AsmUtils.getExtensionAnnotationValue(operation, "binding", true)
                            .orElseThrow(() -> new IllegalArgumentException("Bound operation not defined"));

                    implementation = AsmUtils.getOperationImplementationByName(entityType.get(), boundOperationName)
                            .orElseThrow(() -> new IllegalArgumentException("Operation implementation not found"));

                } else {
                    implementation = AsmUtils.getImplementationClassOfOperation(operation)
                            .orElseThrow(() -> new IllegalArgumentException("Operation implementation not found"));

                    if (exposed) {
                        accessManager.authorizeOperation(operation, null, exchange);
                    }
                }

                if (exchange.containsKey(PRINCIPAL_KEY)) {
                    context.putIfAbsent(PRINCIPAL_KEY, exchange.get(PRINCIPAL_KEY));
                }

                if (exposed) {
                    final List<FeedbackItem> feedbackItems = new ArrayList<>();
                    final List<EParameter> parameters = operation.getEParameters().stream()
                            .filter(parameter -> (parameter.getEType() instanceof EClass))
                            .collect(Collectors.toList());
                    parameters.forEach(parameter -> {
                        final EClass transferObjectType = (EClass) parameter.getEType();
                        final RequestConverter requestConverter = RequestConverter.builder()
                                .transferObjectType(transferObjectType)
                                .coercer(dataTypeManager.getCoercer())
                                .asmUtils(getAsmUtils())
                                .validators(getValidators())
                                .trimString(trimString)
                                .requiredStringValidatorOption(RequestConverter.RequiredStringValidatorOption.valueOf(requiredStringValidatorOption))
                                .identifierSigner(identifierSigner)
                                .identifierProvider(identifierProvider)
                                .throwValidationException(true)
                                .keepProperties(Arrays.asList(IdentifierSigner.SIGNED_IDENTIFIER_KEY, identifierProvider.getName(), REFERENCE_ID_KEY, VERSION_KEY))
                                .filestoreTokenValidator(filestoreTokenValidator)
                                .build();

                        final Map<String, Object> validationContext = new TreeMap<>();
                        validationContext.put(RequestConverter.LOCATION_KEY, parameters.size() != 1 || parameter.isMany() ? parameter.getName() : "");
                        if (AsmUtils.getBehaviour(operation).filter(b -> AsmUtils.OperationBehaviour.CREATE_INSTANCE.equals(b) || AsmUtils.OperationBehaviour.VALIDATE_CREATE.equals(b)).isPresent()) {
                            validationContext.put(RequestConverter.VALIDATE_FOR_CREATE_OR_UPDATE_KEY, true);
                            validationContext.put(RequestConverter.CREATE_REFERENCE_KEY, asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation).orElse(null));
                        } else if (AsmUtils.getBehaviour(operation).filter(b -> AsmUtils.OperationBehaviour.UPDATE_INSTANCE.equals(b) || AsmUtils.OperationBehaviour.VALIDATE_UPDATE.equals(b)).isPresent()) {
                            validationContext.put(RequestConverter.VALIDATE_FOR_CREATE_OR_UPDATE_KEY, true);
                            validationContext.put(RequestConverter.VALIDATE_MISSING_FEATURES_KEY, false);
                        } else if (AsmUtils.getBehaviour(operation).filter(b -> AsmUtils.OperationBehaviour.SET_REFERENCE.equals(b) || AsmUtils.OperationBehaviour.UNSET_REFERENCE.equals(b) || AsmUtils.OperationBehaviour.ADD_REFERENCE.equals(b) || AsmUtils.OperationBehaviour.REMOVE_REFERENCE.equals(b)).isPresent()) {
                            validationContext.put(RequestConverter.NO_TRAVERSE_KEY, true);
                            validationContext.put(RequestConverter.VALIDATE_MISSING_FEATURES_KEY, false);
                        } else if (AsmUtils.getBehaviour(operation).filter(b -> AsmUtils.OperationBehaviour.GET_REFERENCE_RANGE.equals(b) || AsmUtils.OperationBehaviour.GET_INPUT_RANGE.equals(b)).isPresent()) {
                            validationContext.put(RequestConverter.VALIDATE_MISSING_FEATURES_KEY, false); // not necessary because optional type is used for owner instance
                            validationContext.put(RequestConverter.IGNORE_INVALID_VALUES_KEY, true);
                        } else if (AsmUtils.getExtensionAnnotationValue(operation, "inputRange", false).isPresent()) {
                            validationContext.put(RequestConverter.VALIDATE_MISSING_FEATURES_KEY, false);
                            validationContext.put(RequestConverter.IGNORE_INVALID_VALUES_KEY, true);
                        }

                        if (parameter.isMany()) {
							final Collection<Map<String, Object>> parameterList = exchange.get(parameter.getName()) != null ? ((Collection<Map<String, Object>>) exchange.get(parameter.getName())) : Collections.emptyList();
                            final int parameterListSize = parameterList.size();
                            if (parameterListSize < parameter.getLowerBound()) {
                                final Map<String, Object> details = new LinkedHashMap<>();
                                details.put(Validator.FEATURE_KEY, parameter.getName());
                                details.put("size", parameterListSize);
                                feedbackItems.add(FeedbackItem.builder()
                                        .code("TOO_FEW_PARAMETERS")
                                        .level(FeedbackItem.Level.ERROR)
                                        .location(validationContext.get(RequestConverter.LOCATION_KEY))
                                        .details(details)
                                        .build());
                            } else if (parameterListSize > parameter.getUpperBound() && parameter.getUpperBound() != -1) {
                                final Map<String, Object> details = new LinkedHashMap<>();
                                details.put(Validator.FEATURE_KEY, parameter.getName());
                                details.put("size", parameterListSize);
                                feedbackItems.add(FeedbackItem.builder()
                                        .code("TOO_MANY_PARAMETERS")
                                        .level(FeedbackItem.Level.ERROR)
                                        .location(validationContext.get(RequestConverter.LOCATION_KEY))
                                        .details(details)
                                        .build());
                            }
                            int idx = 0;
                            final List<Payload> payloadList = new ArrayList<>();
                            for (Iterator<Map<String, Object>> it = parameterList.iterator(); it.hasNext(); idx++) {
                                validationContext.put(RequestConverter.LOCATION_KEY, parameter.getName() + "[" + idx + "]");
                                final Map<String, Object> input = it.next();
                                Optional<Payload> payload = Optional.empty();
                                if (input != null) {
                                    try {
                                        payload = requestConverter.convert(input, validationContext);
                                    } catch (ValidationException ex) {
                                        feedbackItems.addAll(ex.getFeedbackItems());
                                        continue;
                                    }
                                }
                                if (!payload.isPresent()) {
                                    final Map<String, Object> details = new LinkedHashMap<>();
                                    details.put(Validator.FEATURE_KEY, parameter.getName());
                                    feedbackItems.add(FeedbackItem.builder()
                                            .code("NULL_PARAMETER_ITEM_IS_NOT_SUPPORTED")
                                            .level(FeedbackItem.Level.ERROR)
                                            .location(validationContext.get(RequestConverter.LOCATION_KEY))
                                            .details(details)
                                            .build());
                                } else {
                                    payloadList.add(payload.get());
                                }
                                if (AsmUtils.getBehaviour(operation).filter(b -> AsmUtils.OperationBehaviour.SET_REFERENCE.equals(b) || AsmUtils.OperationBehaviour.ADD_REFERENCE.equals(b)).isPresent()) {
                                    final EReference reference = getAsmUtils().getOwnerOfOperationWithDefaultBehaviour(operation)
                                            .map(o -> (EReference) o)
                                            .get();
                                    feedbackItems.addAll(rangeValidator.validateValue(Payload.asPayload(exchange), reference, Payload.asPayload(input), validationContext));
                                }
                            }
                            exchange.put(parameter.getName(), payloadList);
                        } else {
                            try {
								final Optional<Payload> payload = exchange.get(parameter.getName()) != null ? requestConverter.convert((Map<String, Object>) exchange.get(parameter.getName()), validationContext) : Optional.empty();
                                if (payload.isPresent()) {
                                    exchange.put(parameter.getName(), payload.get());
                                    if (AsmUtils.getBehaviour(operation).filter(b -> AsmUtils.OperationBehaviour.SET_REFERENCE.equals(b)).isPresent()) {
                                        final EReference reference = getAsmUtils().getOwnerOfOperationWithDefaultBehaviour(operation)
                                                .map(o -> (EReference) o)
                                                .get();
                                        feedbackItems.addAll(rangeValidator.validateValue(Payload.asPayload(exchange), reference, payload.get(), validationContext));
                                    }
                                    Optional<String> inputRangeReferenceFQName = AsmUtils.getExtensionAnnotationValue(operation, "inputRange", false);
                                    if (inputRangeReferenceFQName.isPresent()) {
                                        Optional<SignedIdentifier> signedIdentifier = identifierSigner.extractSignedIdentifier(transferObjectType, payload.get());
                                        final Payload inputPayload = getTransferObjectAsBoundType(transferObjectType, signedIdentifier.get());
                                        exchange.put(parameter.getName(), inputPayload);
                                        final EReference reference = getAsmUtils().resolveReference(inputRangeReferenceFQName.get())
                                                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));
                                        feedbackItems.addAll(rangeValidator.validateValue(Payload.asPayload(exchange), reference, inputPayload, validationContext));
                                    }
                                } else if (parameter.isRequired()) {
                                    final Map<String, Object> details = new LinkedHashMap<>();
                                    details.put(Validator.FEATURE_KEY, parameter.getName());
                                    feedbackItems.add(FeedbackItem.builder()
                                            .code("MISSING_REQUIRED_PARAMETER")
                                            .level(FeedbackItem.Level.ERROR)
                                            .location(validationContext.get(RequestConverter.LOCATION_KEY))
                                            .details(details)
                                            .build());
                                }
                            } catch (ValidationException ex) {
                                feedbackItems.addAll(ex.getFeedbackItems());
                            }
                        }
                    });

                    if (!feedbackItems.isEmpty()) {
                        throw new ValidationException("Invalid request", feedbackItems);
                    }
                }

                String callType = SDK;
                String implementationName = AsmUtils.getOperationFQName(implementation);
                final Optional<String> outputParameterName = AsmUtils.getOutputParameterName(implementation);
                checkArgument(outputParameterName.isPresent() || operation.getEType() == null);

                Function<Payload, Payload> operationCall = dispatcherFunctionProvider.getSdkFunctions().get(implementation);
                if (operationCall == null) {
                    callType = SCRIPT;
                    operationCall = dispatcherFunctionProvider.getScriptFunctions().get(implementation);
                }
                if (operationCall == null) {
                    final AsmUtils.OperationBehaviour behaviour = AsmUtils.getBehaviour(operation)
                            .orElseThrow(() -> new IllegalArgumentException("Not implemented yet"));
                    implementationName = behaviour.name();
                    callType = BEHAVIOUR;

                    operationCall = (Payload p) -> {
                        Object _result = getBehaviourCalls().stream()
                                .filter(b -> b.isSuitableForOperation(operation))
                                .findFirst()
                                .orElseThrow(() -> new UnsupportedOperationException("Not supported yet"))
                                .call(exchange, operation);
                        return operation.getEType() != null && _result != null ? Payload.map(outputParameterName.get(), _result) : Payload.empty();
                    };
                }

                // Custom / Script type calls have to be wrapped in transactional context and include this instance (payload)
                if (SCRIPT.equals(callType) || SDK.equals(callType)) {
                    operationCall = new TransactionalCall(transactionManager, operationCall, operation);

                    if (AsmUtils.isBound(operation) && exchange.get(INSTANCE_KEY_OF_BOUND_OPERATION) == null) {
                        final Payload thisMappedTransferObject = getTransferObjectAsBoundType(mappedTransferObjectType.get(), id.get());

                        exchange.put(INSTANCE_KEY_OF_BOUND_OPERATION, thisMappedTransferObject);
                    }
                }

                if (log.isDebugEnabled()) {
                    log.debug("Calling operation: {}, {} implementation: {}, instance: {}, entity type: {}",
                            new Object[]{
                                    callType,
                                    AsmUtils.getOperationFQName(operation),
                                    implementationName,
                                    id.map(_id -> _id.getIdentifier()),
                                    entityType.map(e -> AsmUtils.getClassifierFQName(e)).orElse(null)});
                }

                String measurementKey;
                if (SCRIPT.equals(callType)) {
                    measurementKey = METRICS_SCRIPT_CALL;
                } else if (SDK.equals(callType)) {
                    measurementKey = METRICS_SDK_CALL;
                } else {
                    measurementKey = METRICS_JUDO_CALL;
                }
                try (MetricsCancelToken ct_inner = metricsCollector.start(measurementKey)) {
                    result = operationCall.apply(Payload.asPayload(exchange));
                    MDC.put("operation", operationFullyQualifiedName); // reset operation
                }

                final Optional<AsmUtils.OperationBehaviour> behaviour = AsmUtils.getBehaviour(operation);
                final ETypedElement producedBy;
                if ((AsmUtils.OperationBehaviour.REFRESH.equals(behaviour.orElse(null)) || AsmUtils.OperationBehaviour.UPDATE_INSTANCE.equals(behaviour.orElse(null)))
                        && id.isPresent()) {
                    producedBy = id.get().getProducedBy();
                } else {
                    producedBy = operation;
                }

                if (result != null && result.get(FAULT) != null) {
					final String faultTypeAsString = (String) ((Map<String, Object>) result.get(FAULT)).get(FAULT_TYPE);
					final String errorCode = (String) ((Map<String, Object>) result.get(FAULT)).get(FAULT_ERROR_CODE);
					final Throwable cause = (Throwable) ((Map<String, Object>) result.get(FAULT)).get(FAULT_CAUSE);
                    throw new BusinessException(faultTypeAsString, errorCode, (Map<String, Object>) result.get(FAULT), cause);
                } else if (exposed && outputParameterName.isPresent() && result != null && result.get(outputParameterName.get()) != null) {
                    final ResponseConverter responseConverter = ResponseConverter.builder()
                            .transferObjectType((EClass) operation.getEType())
                            .coercer(dataTypeManager.getCoercer())
                            .asmUtils(getAsmUtils())
                            .keepProperties(Arrays.asList(identifierProvider.getName(), UPDATEABLE_KEY, DELETEABLE_KEY, SELECTED_ITEM_KEY, REFERENCE_ID_KEY, Dispatcher.ENTITY_TYPE_MAP_KEY, VERSION_KEY))
                            .filestoreTokenIssuer(filestoreTokenIssuer)
                            .build();

                    if (operation.isMany()) {
                        final Collection<Payload> payloadList = ((Collection<Map<String, Object>>) result.get(outputParameterName.get())).stream()
                                .map(input -> responseConverter.convert(input).orElse(null))
                                .filter(payload -> payload != null)
                                .collect(Collectors.toList());
                        result.put(outputParameterName.get(), payloadList);
                        payloadList.forEach(payload -> identifierSigner.signIdentifiers(producedBy, payload, immutable));
                    } else {
                        final Payload _result = result;
                        responseConverter.convert((Map<String, Object>) result.get(outputParameterName.get()))
                                .ifPresent((payload) -> {
                                    _result.put(outputParameterName.get(), payload);
                                    identifierSigner.signIdentifiers(producedBy, payload, immutable);
                                });
                    }
                }

                if (log.isTraceEnabled()) {
                    log.trace("Operation result: {}", result);
                }

                return result;
            } catch (ClientException | BusinessException ex) {
                // do not wrap ClientException and BusinessException
                throw ex;
            } catch (RuntimeException ex) {
                throw new InternalServerException("Operation failed", ex);
            } finally {
                final Payload _result = result;
                ofNullable(metricsCollector).ifPresent(mc -> {
                    final Map<String, AtomicLong> metrics = mc.getMetrics();
                    if (_result != null && metrics != null && metricsReturned) {
                        _result.putIfAbsent(Dispatcher.HEADERS_KEY, new ConcurrentHashMap<>());
                        ((Map<String, Object>) _result.get(Dispatcher.HEADERS_KEY)).putAll(metrics.entrySet().stream()
                                .filter(e -> e.getValue() != null)
                                .collect(Collectors.toMap(
                                        e -> "X-" + new StringBuilder(
                                                e.getKey().length())
                                                .append(Character.toTitleCase(e.getKey().charAt(0)))
                                                .append(e.getKey().substring(1))
                                                .toString(),
                                        e -> e.getValue().longValue())));
                    }
                });
                final long duration = System.nanoTime() - startTs;
                if (log.isDebugEnabled()) {
                    log.debug("Operation {} completed in {} ms", operationFullyQualifiedName, duration / 1000000);
                }
                context.put(STATEFUL, stateful);

                // TODO - do not cleanup context of operations called by dispatcher
                if (context != null && exposed) {
                    context.removeAll();
                }
                MDC.remove("operation");
            }
    }

    @Override
    public <S, T> T coerce(S sourceValue, String targetClassName) {
        return dataTypeManager.getCoercer().coerce(sourceValue, targetClassName);
    }

    @Override
    public <S, T> T coerce(S sourceValue, Class<T> targetClass) {
        return dataTypeManager.getCoercer().coerce(sourceValue, targetClass);
    }
}
