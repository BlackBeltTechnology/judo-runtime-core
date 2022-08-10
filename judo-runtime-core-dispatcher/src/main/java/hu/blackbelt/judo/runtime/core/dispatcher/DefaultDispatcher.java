package hu.blackbelt.judo.runtime.core.dispatcher;

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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import hu.blackbelt.judo.dao.api.*;
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
import hu.blackbelt.judo.runtime.core.security.OpenIdConfigurationProvider;
import hu.blackbelt.judo.runtime.core.validator.*;
import hu.blackbelt.osgi.filestore.security.api.*;
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
import static hu.blackbelt.judo.runtime.core.validator.DefaultPayloadValidator.CREATE_REFERENCE_KEY;
import static hu.blackbelt.judo.runtime.core.validator.DefaultPayloadValidator.LOCATION_KEY;
import static hu.blackbelt.judo.runtime.core.validator.Validator.*;
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
    public static final String ASM_EXTENSION_ANNOTATION_INPUT_RANGE = "inputRange";
    public static final String ASM_EXTENSION_ANNOTATION_BINDING = "binding";

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

    private final PayloadValidator payloadValidator;

    private final ValidatorProvider validatorProvider;

    private final Boolean metricsReturned;

    private final Boolean trimString;

    private final Boolean caseInsensitiveLike;

    private Set<BehaviourCall<ID>> behaviourCalls;

    private AsmUtils asmUtils;

    private final Validator rangeValidator;

    @SuppressWarnings("unchecked")
	private void setupBehaviourCalls(DAO<ID> dao, IdentifierProvider<ID> identifierProvider, AsmUtils asmUtils) {
        behaviourCalls = ImmutableSet.<BehaviourCall<ID>>builder()
                .add(
                        new ListCall<>(context, dao, identifierProvider, asmUtils, transactionManager, dataTypeManager.getCoercer(), actorResolver, caseInsensitiveLike),
                        new CreateInstanceCall<>(dao, identifierProvider, asmUtils, transactionManager),
                        new ValidateCreateCall<>(context, dao, identifierProvider, asmUtils, transactionManager),
                        new RefreshCall<>(context, dao, identifierProvider, asmUtils, transactionManager, dataTypeManager.getCoercer(), caseInsensitiveLike),
                        new UpdateInstanceCall<>(dao, identifierProvider, asmUtils, transactionManager, dataTypeManager.getCoercer()),
                        new ValidateUpdateCall<>(context, dao, identifierProvider, asmUtils, transactionManager, dataTypeManager.getCoercer()),
                        new DeleteInstanceCall<>(dao, identifierProvider, asmUtils, transactionManager),
                        new SetReferenceCall<>(dao, identifierProvider, asmUtils, transactionManager),
                        new UnsetReferenceCall<>(dao, identifierProvider, asmUtils, transactionManager),
                        new AddReferenceCall<>(dao, identifierProvider, asmUtils, transactionManager),
                        new RemoveReferenceCall<>(dao, identifierProvider, asmUtils, transactionManager),
                        new GetReferenceRangeCall<>(context, dao, identifierProvider, asmUtils, expressionModel, transactionManager, dataTypeManager.getCoercer(), caseInsensitiveLike),
                        new GetInputRangeCall<>(context, dao, identifierProvider, asmUtils, expressionModel, transactionManager, dataTypeManager.getCoercer(), caseInsensitiveLike),
                        new GetPrincipalCall<>(dao, identifierProvider, asmUtils, actorResolver),
                        new GetTemplateCall<>(dao, asmUtils),
                        new GetMetadataCall<>(asmUtils, () -> openIdConfigurationProvider),
                        new GetUploadTokenCall<>(asmUtils, filestoreTokenIssuer)
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
            @NonNull PayloadValidator payloadValidator,
            ValidatorProvider validatorProvider,
            OpenIdConfigurationProvider openIdConfigurationProvider,
            TokenIssuer filestoreTokenIssuer,
            TokenValidator filestoreTokenValidator,
            AccessManager accessManager,
            TransactionManager transactionManager,
            Boolean metricsReturned,
            Boolean enableValidation,
            Boolean trimString,
            Boolean caseInsensitiveLike
        ) {
        this.asmModel = asmModel;
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.dispatcherFunctionProvider = dispatcherFunctionProvider;
        this.dataTypeManager = dataTypeManager;
        this.identifierSigner = identifierSigner;

        this.accessManager = Objects.requireNonNullElseGet(accessManager, () -> (operation, signedIdentifier, exchange) -> {});

        this.actorResolver = actorResolver;
        this.transactionManager = transactionManager;
        this.expressionModel = expressionModel;
        this.context = context;
        this.metricsCollector = metricsCollector;
        this.payloadValidator = payloadValidator;
        this.validatorProvider = Objects.requireNonNullElseGet(validatorProvider, () -> new DefaultValidatorProvider(dao, identifierProvider, context));

        if (enableValidation != null && !enableValidation) {
            validatorProvider.getValidators().clear();
        }

        rangeValidator = validatorProvider.getInstance(RangeValidator.class).orElseGet(DummyValidator::new);

        this.metricsReturned = Objects.requireNonNullElse(metricsReturned, true);

        this.trimString = Objects.requireNonNullElse(trimString, false);

        this.caseInsensitiveLike = Objects.requireNonNullElse(caseInsensitiveLike, false);

        this.openIdConfigurationProvider = openIdConfigurationProvider;

        this.filestoreTokenIssuer = Objects.requireNonNullElseGet(filestoreTokenIssuer, () -> new TokenIssuer() {
            @Override
            public String createUploadToken(Token<UploadClaim> token) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String createDownloadToken(Token<DownloadClaim> token) {
                throw new UnsupportedOperationException();
            }
        });

        this.filestoreTokenValidator = Objects.requireNonNullElseGet(filestoreTokenValidator, () -> new TokenValidator() {
            @Override
            public Token<UploadClaim> parseUploadToken(String tokenString) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Token<DownloadClaim> parseDownloadToken(String tokenString) {
                throw new UnsupportedOperationException();
            }
        });

        asmUtils = new AsmUtils(asmModel.getResourceSet());
        setupBehaviourCalls(dao, identifierProvider, asmUtils);
        registerDataTypes();
    }

    private void registerDataTypes() {
        getAsmUtils().all(EDataType.class)
                .filter(t -> "byte[]".equals(t.getInstanceClassName()))
                .forEach(t -> dataTypeManager.registerCustomType(t, t.getInstanceClassName(), null, new FileTypeFormatter()));
    }

    @SuppressWarnings("unused")
	private void unregisterDataTypes() {
        getAsmUtils().all(EDataType.class)
                .filter(t -> "byte[]".equals(t.getInstanceClassName()))
                .forEach(dataTypeManager::unregisterCustomType);
    }

    private Optional<SignedIdentifier> getIdForBoundOperation(final EClass mappedTransferObjectType, final Map<String, Object> exchange, final boolean exposed) {
        if (exposed) {
            if (exchange.get(IdentifierSigner.SIGNED_IDENTIFIER_KEY) == null) {
                log.info("Operation failed, missing " + IdentifierSigner.SIGNED_IDENTIFIER_KEY + " header of bound operation");
                throw new ValidationException(Collections.singleton(ValidationResult.builder()
                        .code(ERROR_MISSING_IDENTIFIER_OF_BOUND_OPERATION)
                        .level(ValidationResult.Level.ERROR)
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
                throw new ValidationException(Collections.singleton(ValidationResult.builder()
                        .code(ERROR_INVALID_IDENTIFIER)
                        .level(ValidationResult.Level.ERROR)
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
        checkState((containerOfOperation instanceof EClass)
                        && (getAsmUtils().isMappedTransferObjectType((EClass) containerOfOperation)),
                "Container of bound transfer operation must be a mapped transfer object type");
        return (EClass) containerOfOperation;
    }

    private Payload getTransferObjectAsBoundType(EClass mappedTransferObjectType, SignedIdentifier signedIdentifier) {
        final ID id = dataTypeManager.getCoercer().coerce(signedIdentifier.getIdentifier(), identifierProvider.getType());
        return dao.getByIdentifier(mappedTransferObjectType, id)
                .orElseThrow(() -> new NotFoundException(ValidationResult.builder()
                        .code(ERROR_BOUND_OPERATION_INSTANCE_NOT_FOUND)
                        .level(ValidationResult.Level.ERROR)
                        .build()));
    }

    private void checkProducedByOfBoundOperationInstance(final SignedIdentifier signedIdentifier, final EOperation operation) {
        if (signedIdentifier.getProducedBy() == null) {
            throw new SecurityException("Producer of '" + signedIdentifier.getIdentifier() + "' is missing");
        }

        final ETypedElement producedBy = signedIdentifier.getProducedBy();
        if (operation.eContainer() != null && !AsmUtils.equals(operation.eContainer(), producedBy.getEType()) &&
                !((producedBy.getEType() instanceof EClass) && ((EClass) producedBy.getEType()).getEAllSuperTypes().contains(operation.eContainer()))) {
            log.info("Mapped transfer object type of bound operation {} does not match type of signed identifier {}", AsmUtils.getOperationFQName(operation), AsmUtils.getClassifierFQName(producedBy.getEType()));
            throw new AccessDeniedException(ValidationResult.builder()
                    .code(ERROR_ACCESS_DENIED_INVALID_TYPE)
                    .level(ValidationResult.Level.ERROR)
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

    @SuppressWarnings("unchecked")
    private void processMetrics(final Payload payload) {
        ofNullable(metricsCollector).ifPresent(mc -> {
            final Map<String, AtomicLong> metrics = mc.getMetrics();
            if (payload != null && metrics != null && metricsReturned) {
                payload.putIfAbsent(Dispatcher.HEADERS_KEY, new ConcurrentHashMap<>());
                ((Map<String, Object>) payload.get(Dispatcher.HEADERS_KEY)).putAll(metrics.entrySet().stream()
                        .filter(e -> e.getValue() != null)
                        .collect(Collectors.toMap(
                                e -> "X-" + Character.toTitleCase(e.getKey().charAt(0)) +
                                        e.getKey().substring(1),
                                e -> e.getValue().longValue())));
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void processFault(final Payload result, String outputParameterName, EClassifier operationType, boolean exposed, ETypedElement producedBy, boolean immutable, boolean isMany) {
        if (result != null && result.get(FAULT) != null) {
            Map<String, Object> fault = (Map<String, Object>) result.get(FAULT);
            throw new BusinessException((String) fault.get(FAULT_TYPE), (String) fault.get(FAULT_ERROR_CODE), fault, (Throwable) fault.get(FAULT_CAUSE));
        } else if (exposed && outputParameterName != null && result != null && result.get(outputParameterName) != null) {
            final ResponseConverter responseConverter = ResponseConverter.builder()
                    .transferObjectType((EClass) operationType)
                    .coercer(dataTypeManager.getCoercer())
                    .asmUtils(getAsmUtils())
                    .keepProperties(Arrays.asList(identifierProvider.getName(), UPDATEABLE_KEY, DELETEABLE_KEY, SELECTED_ITEM_KEY, REFERENCE_ID_KEY, Dispatcher.ENTITY_TYPE_MAP_KEY, VERSION_KEY))
                    .filestoreTokenIssuer(filestoreTokenIssuer)
                    .build();

            if (isMany) {
                final Collection<Payload> payloadList = ((Collection<Map<String, Object>>) result.get(outputParameterName)).stream()
                        .map(input -> responseConverter.convert(input).orElse(null))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                result.put(outputParameterName, payloadList);
                payloadList.forEach(payload -> identifierSigner.signIdentifiers(producedBy, payload, immutable));
            } else {
                responseConverter.convert((Map<String, Object>) result.get(outputParameterName))
                        .ifPresent((payload) -> {
                            result.put(outputParameterName, payload);
                            identifierSigner.signIdentifiers(producedBy, payload, immutable);
                        });
            }
        }
    }

    @SuppressWarnings("unchecked")
    void processParameter(EOperation operation, EParameter parameter, EClassifier parameterType, Map<String, Object> exchange, final Map<String, Object> validationContext, List<ValidationResult> ValidationResults) {
        final EClass transferObjectType = (EClass) parameterType;
        final RequestConverter requestConverter = RequestConverter.builder()
                .transferObjectType(transferObjectType)
                .coercer(dataTypeManager.getCoercer())
                .asmUtils(getAsmUtils())
                .validatorProvider(validatorProvider)
                .trimString(trimString)
                .identifierSigner(identifierSigner)
                .identifierProvider(identifierProvider)
                .payloadValidator(payloadValidator)
                .throwValidationException(true)
                .keepProperties(Arrays.asList(IdentifierSigner.SIGNED_IDENTIFIER_KEY, identifierProvider.getName(), REFERENCE_ID_KEY, VERSION_KEY))
                .filestoreTokenValidator(filestoreTokenValidator)
                .build();

        AsmUtils.OperationBehaviour operationBehaviour = AsmUtils.getBehaviour(operation).orElse(null);
        setupValidationContextForBoundOperation(operationBehaviour, operation, validationContext);

        if (parameter.isMany()) {
            final Collection<Map<String, Object>> parameterList = exchange.get(parameter.getName()) != null ? ((Collection<Map<String, Object>>) exchange.get(parameter.getName())) : Collections.emptyList();
            final int parameterListSize = parameterList.size();
            if (parameterListSize < parameter.getLowerBound()) {
                addValidationError(ImmutableMap.of(
                            Validator.FEATURE_KEY, parameter.getName(),
                            SIZE_PARAMETER, parameterListSize
                        ),
                        validationContext.get(LOCATION_KEY),
                        ValidationResults,
                        ERROR_TOO_FEW_PARAMETERS);
            } else if (parameterListSize > parameter.getUpperBound() && parameter.getUpperBound() != -1) {
                addValidationError(ImmutableMap.of(
                        Validator.FEATURE_KEY, parameter.getName(),
                                SIZE_PARAMETER, parameterListSize
                        ),
                        validationContext.get(LOCATION_KEY),
                        ValidationResults,
                        ERROR_TOO_MANY_PARAMETERS);
            }
            int idx = 0;
            final List<Payload> payloadList = new ArrayList<>();
            for (Iterator<Map<String, Object>> it = parameterList.iterator(); it.hasNext(); idx++) {
                validationContext.put(LOCATION_KEY, parameter.getName() + "[" + idx + "]");
                final Map<String, Object> input = it.next();
                Optional<Payload> payload = Optional.empty();
                if (input != null) {
                    try {
                        payload = requestConverter.convert(input, validationContext);
                    } catch (ValidationException ex) {
                        ValidationResults.addAll(ex.getValidationResults());
                        continue;
                    }
                }
                if (payload.isEmpty()) {
                    addValidationError(
                            ImmutableMap.of(Validator.FEATURE_KEY, parameter.getName()),
                            validationContext.get(LOCATION_KEY),
                            ValidationResults,
                            ERROR_NULL_PARAMETER_ITEM_IS_NOT_SUPPORTED);
                } else {
                    payloadList.add(payload.get());
                }
                if (AsmUtils.OperationBehaviour.SET_REFERENCE.equals(operationBehaviour) || AsmUtils.OperationBehaviour.ADD_REFERENCE.equals(operationBehaviour)) {
                    final EReference reference = getAsmUtils().getOwnerOfOperationWithDefaultBehaviour(operation)
                            .map(o -> (EReference) o)
                            .orElseThrow(() -> new IllegalArgumentException("Owner operation is not found for " + operation.getName()));
                    ValidationResults.addAll(rangeValidator.validateValue(Payload.asPayload(exchange), reference, Payload.asPayload(input), validationContext));
                }
            }
            exchange.put(parameter.getName(), payloadList);
        } else {
            try {
                final Optional<Payload> payload = exchange.get(parameter.getName()) != null ? requestConverter.convert((Map<String, Object>) exchange.get(parameter.getName()), validationContext) : Optional.empty();
                if (payload.isPresent()) {
                    exchange.put(parameter.getName(), payload.get());
                    if (AsmUtils.OperationBehaviour.SET_REFERENCE.equals(operationBehaviour)) {
                        final EReference reference = getAsmUtils().getOwnerOfOperationWithDefaultBehaviour(operation)
                                .map(o -> (EReference) o)
                                .orElseThrow(() -> new IllegalArgumentException("Owner operation is not found for " + operation.getName()));
                        ValidationResults.addAll(rangeValidator.validateValue(Payload.asPayload(exchange), reference, payload.get(), validationContext));
                    }
                    Optional<String> inputRangeReferenceFQName = AsmUtils.getExtensionAnnotationValue(operation, ASM_EXTENSION_ANNOTATION_INPUT_RANGE, false);
                    if (inputRangeReferenceFQName.isPresent()) {
                        Optional<SignedIdentifier> signedIdentifier = identifierSigner.extractSignedIdentifier(transferObjectType, payload.get());
                        final Payload inputPayload = getTransferObjectAsBoundType(transferObjectType, signedIdentifier.orElseThrow(() -> new IllegalArgumentException("Missing ID of bound operation")));
                        exchange.put(parameter.getName(), inputPayload);
                        final EReference reference = getAsmUtils().resolveReference(inputRangeReferenceFQName.get())
                                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));
                        ValidationResults.addAll(rangeValidator.validateValue(Payload.asPayload(exchange), reference, inputPayload, validationContext));
                    }
                } else if (parameter.isRequired()) {
                    addValidationError(ImmutableMap.of(Validator.FEATURE_KEY, parameter.getName()),
                            validationContext.get(LOCATION_KEY),
                            ValidationResults,
                            ERROR_MISSING_REQUIRED_PARAMETER);
                }
            } catch (ValidationException ex) {
                ValidationResults.addAll(ex.getValidationResults());
            }
        }
    }

    private void setupValidationContextForBoundOperation(AsmUtils.OperationBehaviour operationBehaviour, EOperation operation, Map<String, Object> validationContext) {
        if (AsmUtils.OperationBehaviour.CREATE_INSTANCE.equals(operationBehaviour)
                || AsmUtils.OperationBehaviour.VALIDATE_CREATE.equals(operationBehaviour)) {
            validationContext.put(RequestConverter.VALIDATE_FOR_CREATE_OR_UPDATE_KEY, true);
            validationContext.put(CREATE_REFERENCE_KEY, asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation).orElse(null));
        } else if (AsmUtils.OperationBehaviour.UPDATE_INSTANCE.equals(operationBehaviour)
                || AsmUtils.OperationBehaviour.VALIDATE_UPDATE.equals(operationBehaviour)) {
            validationContext.put(RequestConverter.VALIDATE_FOR_CREATE_OR_UPDATE_KEY, true);
            validationContext.put(RequestConverter.VALIDATE_MISSING_FEATURES_KEY, false);
        } else if (AsmUtils.OperationBehaviour.SET_REFERENCE.equals(operationBehaviour)
                || AsmUtils.OperationBehaviour.UNSET_REFERENCE.equals(operationBehaviour)
                || AsmUtils.OperationBehaviour.ADD_REFERENCE.equals(operationBehaviour)
                || AsmUtils.OperationBehaviour.REMOVE_REFERENCE.equals(operationBehaviour)) {
            validationContext.put(RequestConverter.NO_TRAVERSE_KEY, true);
            validationContext.put(RequestConverter.VALIDATE_MISSING_FEATURES_KEY, false);
        } else if (AsmUtils.OperationBehaviour.GET_REFERENCE_RANGE.equals(operationBehaviour) || AsmUtils.OperationBehaviour.GET_INPUT_RANGE.equals(operationBehaviour)) {
            validationContext.put(RequestConverter.VALIDATE_MISSING_FEATURES_KEY, false); // not necessary because optional type is used for owner instance
            validationContext.put(RequestConverter.IGNORE_INVALID_VALUES_KEY, true);
        } else if (AsmUtils.getExtensionAnnotationValue(operation, ASM_EXTENSION_ANNOTATION_INPUT_RANGE, false).isPresent()) {
            validationContext.put(RequestConverter.VALIDATE_MISSING_FEATURES_KEY, false);
            validationContext.put(RequestConverter.IGNORE_INVALID_VALUES_KEY, true);
        }

    }

    private Optional<EClass> getEntityType(EClass mappedTransferObjectType, SignedIdentifier signedIdentifier) {
        Optional<EClass> entityType;
        if (signedIdentifier.getEntityType() != null) {
            entityType = Optional.of(getAsmUtils().resolve(signedIdentifier.getEntityType()).filter(t -> t instanceof EClass).map(t -> (EClass) t)
                    .orElseThrow(() -> new IllegalArgumentException("Unable to resolve entity type")));
            final Payload metadata = dao.getMetadata(mappedTransferObjectType, dataTypeManager.getCoercer().coerce(signedIdentifier.getIdentifier(), identifierProvider.getType()))
                    .orElseThrow(() -> new NotFoundException(ValidationResult.builder()
                            .code(ERROR_BOUND_OPERATION_INSTANCE_NOT_FOUND)
                            .level(ValidationResult.Level.ERROR)
                            .build()));
            checkState(Objects.equals(AsmUtils.getClassifierFQName(entityType.get()), metadata.get(Dispatcher.ENTITY_TYPE_MAP_KEY)), "Invalid entity type in signed identifier");
        } else {
            final Payload metadata = getTransferObjectAsBoundType(mappedTransferObjectType, signedIdentifier);
            final String entityTypeFQName = metadata.getAs(String.class, ENTITY_TYPE_MAP_KEY);
            checkArgument(entityTypeFQName != null, "Entity type is unknown");

            entityType = Optional.of(getAsmUtils().resolve(entityTypeFQName).filter(t -> t instanceof EClass).map(t -> (EClass) t)
                    .orElseThrow(() -> new IllegalArgumentException("Unable to resolve entity type")));
        }
        return entityType;
    }

	@Override
    public Map<String, Object> callOperation(final String operationFullyQualifiedName, final Map<String, Object> exchange) {
        MDC.put("operation", operationFullyQualifiedName);
        Payload result = null;

        final long startTs = System.nanoTime();


            final boolean exposed = Boolean.TRUE.equals(exchange.remove("__exposed"));
            final Boolean stateful = context.getAs(Boolean.class, STATEFUL);

            // TODO - do not cleanup context of operations called by dispatcher
            if (exposed) {
                context.removeAll();
            }

            try (MetricsCancelToken ignored = metricsCollector.start(METRICS_DISPATCHER)) {
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

                Optional<SignedIdentifier> signedIdentifier = Optional.empty();
                Optional<EClass> entityType = Optional.empty();
                EOperation implementation;
                EClass mappedTransferObjectType = null;
                Optional<AsmUtils.OperationBehaviour> behaviour = AsmUtils.getBehaviour(operation);

                if (AsmUtils.isBound(operation)) {
                    mappedTransferObjectType = getContainerOfBoundOperation(operation);

                    signedIdentifier = getIdForBoundOperation(mappedTransferObjectType, exchange, exposed);
                    SignedIdentifier signedIdentifierValue = signedIdentifier.orElseThrow(() -> new IllegalArgumentException("Missing ID of bound operation"));
                    if (exposed) {
                        if (Boolean.TRUE.equals(signedIdentifierValue.getImmutable())) {
                            throw new AccessDeniedException(ValidationResult.builder()
                                    .code(ERROR_BOUND_OPERATION_INSTANCE_IS_IMMUTABLE)
                                    .level(ValidationResult.Level.ERROR)
                                    .build());
                        }
                        accessManager.authorizeOperation(operation, signedIdentifierValue, exchange);
                        checkProducedByOfBoundOperationInstance(signedIdentifierValue, operation);
                    }

                    entityType = getEntityType(mappedTransferObjectType, signedIdentifierValue);

                    final String boundOperationName = AsmUtils.getExtensionAnnotationValue(operation, ASM_EXTENSION_ANNOTATION_BINDING, true)
                            .orElseThrow(() -> new IllegalArgumentException("Bound operation not defined"));

                    EClass finalMappedTransferObjectType = mappedTransferObjectType;
                    implementation = AsmUtils.getOperationImplementationByName(entityType.orElseThrow(() -> new IllegalArgumentException("Missing entity type for " + finalMappedTransferObjectType.getName())), boundOperationName)
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

                // Validating and converting parameters
                if (exposed) {
                    final List<ValidationResult> ValidationResults = new ArrayList<>();
                    final List<EParameter> parameters = operation.getEParameters().stream()
                            .filter(parameter -> (parameter.getEType() instanceof EClass))
                            .collect(Collectors.toList());

                    parameters.forEach(parameter -> {
                        final EClass transferObjectType = (EClass) parameter.getEType();
                        final Map<String, Object> validationContext = new TreeMap<>();
                        validationContext.put(LOCATION_KEY, parameters.size() != 1 || parameter.isMany() ? parameter.getName() : "");
                        processParameter(operation, parameter, transferObjectType, exchange, validationContext, ValidationResults);
                    });

                    if (!ValidationResults.isEmpty()) {
                        throw new ValidationException("Invalid request", ValidationResults);
                    }
                }

                String callType = SDK;
                String measurementKey = METRICS_SDK_CALL;

                String implementationName = AsmUtils.getOperationFQName(implementation);
                final Optional<String> outputParameterName = AsmUtils.getOutputParameterName(implementation);
                final EClassifier operationType = operation.getEType();


                checkArgument(outputParameterName.isPresent() || operationType == null);

                // 1. Check for SDK defined operation implementation
                Function<Payload, Payload> operationCall = dispatcherFunctionProvider.getSdkFunctions().get(implementation);

                // 2. Check for script operation function
                if (operationCall == null) {
                    callType = SCRIPT;
                    measurementKey = METRICS_SCRIPT_CALL;
                    operationCall = dispatcherFunctionProvider.getScriptFunctions().get(implementation);
                }

                // 3. Check for default behaviour function
                if (operationCall == null) {
                    implementationName = behaviour.orElseThrow(() -> new IllegalArgumentException("Not implemented yet")).name();
                    callType = BEHAVIOUR;
                    measurementKey = METRICS_JUDO_CALL;

                    operationCall = (Payload p) -> {
                        Object behaviourOperationCallResult = getBehaviourCalls().stream()
                                .filter(b -> b.isSuitableForOperation(operation))
                                .findFirst()
                                .orElseThrow(() -> new UnsupportedOperationException("Not supported yet"))
                                .call(exchange, operation);

                        return operationType != null && behaviourOperationCallResult != null
                                ? Payload.map(outputParameterName.get(), behaviourOperationCallResult)
                                : Payload.empty();
                    };
                }

                // Custom / Script type calls have to be wrapped in transactional context and include this instance (payload)
                if (SCRIPT.equals(callType) || SDK.equals(callType)) {
                    operationCall = new TransactionalCall(transactionManager, operationCall, operation);

                    if (AsmUtils.isBound(operation) && exchange.get(INSTANCE_KEY_OF_BOUND_OPERATION) == null) {
                        final Payload thisMappedTransferObject = getTransferObjectAsBoundType(mappedTransferObjectType, signedIdentifier.orElseThrow(() -> new IllegalArgumentException("Missing ID of bound operation")));
                        exchange.put(INSTANCE_KEY_OF_BOUND_OPERATION, thisMappedTransferObject);
                    }
                }

                if (log.isDebugEnabled()) {
                    log.debug("Calling operation: {}, {} implementation: {}, instance: {}, entity type: {}",
                            callType,
                            AsmUtils.getOperationFQName(operation),
                            implementationName,
                            signedIdentifier.map(SignedIdentifier::getIdentifier),
                            entityType.map(AsmUtils::getClassifierFQName).orElse(null));
                }

                try (MetricsCancelToken ignored1 = metricsCollector.start(measurementKey)) {
                    result = operationCall.apply(Payload.asPayload(exchange));
                    MDC.put("operation", operationFullyQualifiedName); // reset operation
                }

                final ETypedElement producedBy;
                if ((AsmUtils.OperationBehaviour.REFRESH.equals(behaviour.orElse(null)) || AsmUtils.OperationBehaviour.UPDATE_INSTANCE.equals(behaviour.orElse(null)))
                        && signedIdentifier.isPresent()) {
                    producedBy = signedIdentifier.get().getProducedBy();
                } else {
                    producedBy = operation;
                }

                processFault(result, outputParameterName.orElse(null), operationType, exposed, producedBy, immutable, operation.isMany());
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
                processMetrics(result);
                final long duration = System.nanoTime() - startTs;
                if (log.isDebugEnabled()) {
                    log.debug("Operation {} completed in {} ms", operationFullyQualifiedName, duration / 1000000);
                }
                context.put(STATEFUL, stateful);

                // TODO - do not cleanup context of operations called by dispatcher
                if (exposed) {
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
