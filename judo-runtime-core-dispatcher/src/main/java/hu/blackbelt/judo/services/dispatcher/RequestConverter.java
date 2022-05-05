package hu.blackbelt.judo.services.dispatcher;

import com.google.gson.Gson;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.FileType;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.services.core.PayloadTraverser;
import hu.blackbelt.judo.services.core.exception.FeedbackItem;
import hu.blackbelt.judo.services.core.exception.ValidationException;
import hu.blackbelt.judo.services.dispatcher.behaviours.GetUploadTokenCall;
import hu.blackbelt.judo.services.dispatcher.security.IdentifierSigner;
import hu.blackbelt.judo.services.dispatcher.validators.Validator;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.osgi.filestore.security.api.DownloadClaim;
import hu.blackbelt.osgi.filestore.security.api.Token;
import hu.blackbelt.osgi.filestore.security.api.TokenValidator;
import hu.blackbelt.osgi.filestore.security.api.exceptions.InvalidTokenException;
import lombok.Builder;
import lombok.NonNull;
import lombok.Setter;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.*;

import java.util.*;
import java.util.function.Function;

@Builder
@Slf4j
public class RequestConverter {

    public enum RequiredStringValidatorOption {
        ACCEPT_EMPTY, ACCEPT_NON_EMPTY
    }

    @NonNull
    private final EClass transferObjectType;

    @NonNull
    private final AsmUtils asmUtils;

    @NonNull
    private final Coercer coercer;

    private final TokenValidator filestoreTokenValidator;

    private final boolean trimString;

    private final RequiredStringValidatorOption requiredStringValidatorOption;

    private final IdentifierSigner identifierSigner;

    private final IdentifierProvider identifierProvider;

    private final Collection<Validator> validators;

    @Setter
    private final boolean throwValidationException;

    @NonNull
    @Singular
    private final Collection<String> keepProperties;

    public static final String VALIDATION_RESULT_KEY = "validationResult";
    public static final String LOCATION_KEY = "location";
    public static final String CREATE_REFERENCE_KEY = "createReference";
    public static final String NO_TRAVERSE_KEY = "noTraverse";
    public static final String VALIDATE_FOR_CREATE_OR_UPDATE_KEY = "validateForCreateOrUpdate";
    public static final String VALIDATE_MISSING_FEATURES_KEY = "validateMissingFeatures";
    public static final String IGNORE_INVALID_VALUES_KEY = "ignoreInvalidValues";

    private static final boolean VALIDATE_FOR_CREATE_OR_UPDATE_DEFAULT = false;
    private static final boolean NO_TRAVERSE_DEFAULT = false;
    private static final boolean VALIDATE_FOR_UPDATE_DEFAULT = false;
    private static final boolean VALIDATE_MISSING_FEATURES_DEFAULT = true;
    private static final boolean IGNORE_INVALID_VALUES_DEFAULT = false;

    public static final Function<EAttribute, String> ATTRIBUTE_TO_MODEL_TYPE = attribute -> AsmUtils.getAttributeFQName(attribute).replaceAll("[^a-zA-Z0-9_]", "_");
    public static final Function<EReference, String> REFERENCE_TO_MODEL_TYPE = reference -> AsmUtils.getReferenceFQName(reference).replaceAll("[^a-zA-Z0-9_]", "_");

    public Optional<Payload> convert(final Map<String, Object> input, final Map<String, Object> validationContext) throws ValidationException {
        if (input == null) {
            return Optional.empty();
        }

        final List<FeedbackItem> feedbackItems = new ArrayList<>();

        final Map<String, Object> lastContext = new TreeMap<>(validationContext);
        feedbackItems.addAll(extractIdentifier(transferObjectType, input, lastContext));

        Payload converted = null;
        try {
            converted = PayloadTraverser.builder()
                    .predicate((reference) -> (Boolean) validationContext.getOrDefault(VALIDATE_FOR_CREATE_OR_UPDATE_KEY, VALIDATE_FOR_CREATE_OR_UPDATE_DEFAULT)
                            ? asmUtils.getMappedReference(reference).filter(mappedReference -> mappedReference.isContainment()).isPresent()
                            : AsmUtils.isEmbedded(reference) && !(Boolean) validationContext.getOrDefault(NO_TRAVERSE_KEY, NO_TRAVERSE_DEFAULT))
                    .processor((instance, ctx) -> {
                        final String containerLocation = (String) validationContext.getOrDefault(LOCATION_KEY, "");
                        final Map<String, Object> currentContext = new TreeMap<>(validationContext);
                        currentContext.put(LOCATION_KEY, (containerLocation.isEmpty() ? "" : containerLocation + "/") + ctx.getPathAsString());
                        lastContext.put(LOCATION_KEY, currentContext.get(LOCATION_KEY));

                        // do not validate referenced elements but containment only
                        final boolean validate = !validators.isEmpty() && ctx.getPath().stream().allMatch(e -> e.getReference().isContainment());
                        final boolean ignoreInvalidValues = (Boolean) validationContext.getOrDefault(IGNORE_INVALID_VALUES_KEY, IGNORE_INVALID_VALUES_DEFAULT);
                        ctx.getType().getEAllAttributes().stream().forEach(a -> feedbackItems.addAll(convertValue(a, instance, validate, currentContext, ignoreInvalidValues)));
                        if ((Boolean) validationContext.getOrDefault(VALIDATE_FOR_CREATE_OR_UPDATE_KEY, VALIDATE_FOR_CREATE_OR_UPDATE_DEFAULT) && identifierProvider != null) {
                            ctx.getType().getEAllReferences().stream()
                                    .filter(r -> AsmUtils.isEmbedded(r) && !AsmUtils.isAllowedToCreateEmbeddedObject(r) && asmUtils.getMappedReference(r).filter(mr -> mr.isContainment()).isPresent())
                                    .forEach(c -> {
                                        if (c.isMany()) {
                                            final Collection<Payload> containments = instance.getAsCollectionPayload(c.getName());
                                            if (containments != null) {
                                                for (final Iterator<Payload> it = containments.iterator(); it.hasNext(); ) {
                                                    final Payload p = it.next();
                                                    if (p == null || p.get(identifierProvider.getName()) == null && p.get(IdentifierSigner.SIGNED_IDENTIFIER_KEY) == null) {
                                                        it.remove();
                                                        log.debug("Removed non-createable, not existing reference: {}, instance: {}", AsmUtils.getReferenceFQName(c), p);
                                                    }
                                                }
                                            }
                                        } else {
                                            final Payload containment = instance.getAsPayload(c.getName());
                                            if (containment != null && containment.get(identifierProvider.getName()) == null && containment.get(IdentifierSigner.SIGNED_IDENTIFIER_KEY) == null) {
                                                instance.remove(c.getName());
                                                log.debug("Removed non-createable, not existing reference: {}, instance: {}", AsmUtils.getReferenceFQName(c), containment);
                                            }
                                        }
                                    });
                        }
                        if (validate) {
                            ctx.getType().getEAllReferences().forEach(r -> {
                                final String referenceLocation = currentContext.get(LOCATION_KEY) + (((String) currentContext.get(LOCATION_KEY)).isEmpty() || ((String) currentContext.get(LOCATION_KEY)).endsWith("/") ? "" : ".");
                                final Map<String, Object> referenceValidationContext = new TreeMap<>(lastContext);

                                if (instance.get(r.getName()) instanceof Payload && !r.isMany()) {
                                    referenceValidationContext.put(LOCATION_KEY, referenceLocation + r.getName());
                                    feedbackItems.addAll(extractIdentifier(r.getEReferenceType(), instance.getAsPayload(r.getName()), referenceValidationContext));
                                } else if (instance.get(r.getName()) instanceof Collection && r.isMany()) {
                                    int idx = 0;
                                    for (Iterator<Payload> it = instance.getAsCollectionPayload(r.getName()).iterator(); it.hasNext(); idx++) {
                                        referenceValidationContext.put(LOCATION_KEY, referenceLocation + r.getName() + "[" + idx + "]");
                                        feedbackItems.addAll(extractIdentifier(r.getEReferenceType(), it.next(), referenceValidationContext));
                                    }
                                }
                                feedbackItems.addAll(validateReference(r, instance, currentContext, ignoreInvalidValues));
                            });
                        }
                        for (final Iterator<Map.Entry<String, Object>> it = instance.entrySet().iterator(); it.hasNext(); ) {
                            final Map.Entry<String, Object> entry = it.next();
                            if (!ctx.getType().getEAllStructuralFeatures().stream().anyMatch(f -> Objects.equals(f.getName(), entry.getKey()))
                                    && !keepProperties.contains(entry.getKey())) {
                                it.remove();
                            }
                        }
                    })
                    .build()
                    .traverse(Payload.asPayload(input), transferObjectType);
        } catch (IllegalArgumentException ex) {
            log.debug("Invalid payload", ex);
        }

        if (throwValidationException && !feedbackItems.isEmpty()) {
            throw new ValidationException("Validation failed", Collections.unmodifiableCollection(feedbackItems));
        } else {
            validationContext.put(VALIDATION_RESULT_KEY, feedbackItems);
        }

        return Optional.ofNullable(converted);
    }

    private List<FeedbackItem> extractIdentifier(final EClass clazz, final Map<String, Object> instance, final Map<String, Object> validationContext) {
        if (instance == null) {
            return Collections.emptyList();
        }

        final List<FeedbackItem> feedbackItems = new ArrayList<>();

        if (identifierSigner != null) {
            try {
                identifierSigner.extractSignedIdentifier(clazz, instance);
            } catch (RuntimeException ex) {
                log.info("Invalid signature: {}", ex.getMessage());
                if (log.isDebugEnabled()) {
                    log.debug("Extracting signature failed", ex);
                }

                final Map<String, Object> details = new LinkedHashMap<>();
                if (instance.containsKey(DefaultDispatcher.REFERENCE_ID_KEY)) {
                    details.put(DefaultDispatcher.REFERENCE_ID_KEY, instance.get(DefaultDispatcher.REFERENCE_ID_KEY));
                }
                feedbackItems.add(FeedbackItem.builder()
                        .code("INVALID_IDENTIFIER")
                        .level(FeedbackItem.Level.ERROR)
                        .location(validationContext.get(LOCATION_KEY))
                        .details(details)
                        .build());
            }
        }
        return feedbackItems;
    }

    private List<FeedbackItem> convertValue(final EAttribute attribute, final Payload instance, final boolean validate, final Map<String, Object> validationContext, final boolean ignoreInvalidValue) {
        final Object value = instance.get(attribute.getName());
        final List<FeedbackItem> feedbackItems = new ArrayList<>();

        final String containerLocation = (String) validationContext.getOrDefault(LOCATION_KEY, "");
        final Map<String, Object> currentContext = new TreeMap<>(validationContext);
        currentContext.put(LOCATION_KEY, containerLocation + (containerLocation.isEmpty() || containerLocation.endsWith("/") ? "" : ".") + attribute.getName());

        Object converted = null;
        try {
            if (value != null) {
                converted = attribute.getEAttributeType() instanceof EEnum
                        ? convertEnumerationValue(attribute.getEAttributeType(), value)
                        : convertNonEnumerationValue(attribute.getEAttributeType(), value);

                if (trimString && converted instanceof String && AsmUtils.isString(attribute.getEAttributeType())) {
                    converted = ((String) converted).trim();
                }
            } else {
                converted = null;
            }
        } catch (InvalidTokenException ex) {
            final Map<String, Object> details = new LinkedHashMap<>();
            details.put(Validator.FEATURE_KEY, ATTRIBUTE_TO_MODEL_TYPE.apply(attribute));
            details.put(Validator.VALUE_KEY, value);
            if (instance.containsKey(DefaultDispatcher.REFERENCE_ID_KEY)) {
                details.put(DefaultDispatcher.REFERENCE_ID_KEY, instance.get(DefaultDispatcher.REFERENCE_ID_KEY));
            }
            feedbackItems.add(FeedbackItem.builder()
                    .code("INVALID_FILE_TOKEN")
                    .level(FeedbackItem.Level.ERROR)
                    .location(currentContext.get(LOCATION_KEY))
                    .details(details)
                    .build());
        } catch (RuntimeException ex) {
            if (!ignoreInvalidValue) {
                if (!validate) {
                    throw ex;
                } else {
                    final Map<String, Object> details = new LinkedHashMap<>();
                    details.put(Validator.FEATURE_KEY, ATTRIBUTE_TO_MODEL_TYPE.apply(attribute));
                    details.put(Validator.VALUE_KEY, value);
                    if (instance.containsKey(DefaultDispatcher.REFERENCE_ID_KEY)) {
                        details.put(DefaultDispatcher.REFERENCE_ID_KEY, instance.get(DefaultDispatcher.REFERENCE_ID_KEY));
                    }
                    feedbackItems.add(FeedbackItem.builder()
                            .code("CONVERSION_FAILED")
                            .level(FeedbackItem.Level.ERROR)
                            .location(currentContext.get(LOCATION_KEY))
                            .details(details)
                            .build());
                }
            } else {
                log.debug("Ignored invalid value of {}: {}", new Object[] {AsmUtils.getAttributeFQName(attribute), value}, ex);
            }
        }

        if (instance.containsKey(attribute.getName())) {
            instance.put(attribute.getName(), converted);
        }

        if (validate && !ignoreInvalidValue) {
            feedbackItems.addAll(validateAttribute(attribute, instance, currentContext));
        }

        return feedbackItems;
    }

    private List<FeedbackItem> validateReference(final EReference reference, final Payload instance, final Map<String, Object> validationContext, final boolean ignoreInvalidValues) {
        final Object value = instance.get(reference.getName());
        final List<FeedbackItem> feedbackItems = new ArrayList<>();

        final String containerLocation = (String) validationContext.getOrDefault(LOCATION_KEY, "");
        final Map<String, Object> currentContext = new TreeMap<>(validationContext);
        currentContext.put(LOCATION_KEY, containerLocation + (containerLocation.isEmpty() || containerLocation.endsWith("/") ? "" : ".") + reference.getName());
        final EReference createReference = (EReference) validationContext.get(CREATE_REFERENCE_KEY);
        final boolean validateMissingFeatures = (Boolean) validationContext.getOrDefault(VALIDATE_MISSING_FEATURES_KEY, VALIDATE_MISSING_FEATURES_DEFAULT);

        if (reference.isMany()) {
            if (!ignoreInvalidValues && value instanceof Collection) {
                final int size = ((Collection) value).size();
                if (size < reference.getLowerBound()) {
                    final Map<String, Object> details = new LinkedHashMap<>();
                    details.put(Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference));
                    details.put("size", size);
                    if (instance.containsKey(DefaultDispatcher.REFERENCE_ID_KEY)) {
                        details.put(DefaultDispatcher.REFERENCE_ID_KEY, instance.get(DefaultDispatcher.REFERENCE_ID_KEY));
                    }
                    feedbackItems.add(FeedbackItem.builder()
                            .code("TOO_FEW_ITEMS")
                            .level(FeedbackItem.Level.ERROR)
                            .location(currentContext.get(LOCATION_KEY))
                            .details(details)
                            .build());
                } else if (size > reference.getUpperBound() && reference.getUpperBound() != -1) {
                    final Map<String, Object> details = new LinkedHashMap<>();
                    details.put(Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference));
                    details.put("size", size);
                    if (instance.containsKey(DefaultDispatcher.REFERENCE_ID_KEY)) {
                        details.put(DefaultDispatcher.REFERENCE_ID_KEY, instance.get(DefaultDispatcher.REFERENCE_ID_KEY));
                    }
                    feedbackItems.add(FeedbackItem.builder()
                            .code("TOO_MANY_ITEMS")
                            .level(FeedbackItem.Level.ERROR)
                            .location(currentContext.get(LOCATION_KEY))
                            .details(details)
                            .build());
                }
                int idx = 0;
                for (Iterator it = ((Collection) value).iterator(); it.hasNext(); idx++) {
                    final Map<String, Object> currentItemContext = new TreeMap<>(currentContext);
                    currentItemContext.put(LOCATION_KEY, currentContext.get(LOCATION_KEY) + "[" + idx + "]");

                    final Object item = it.next();
                    if (item == null) {
                        final Map<String, Object> details = new LinkedHashMap<>();
                        details.put(Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference));
                        if (instance.containsKey(DefaultDispatcher.REFERENCE_ID_KEY)) {
                            details.put(DefaultDispatcher.REFERENCE_ID_KEY, instance.get(DefaultDispatcher.REFERENCE_ID_KEY));
                        }
                        feedbackItems.add(FeedbackItem.builder()
                                .code("NULL_ITEM_IS_NOT_SUPPORTED")
                                .level(FeedbackItem.Level.ERROR)
                                .location(currentItemContext.get(LOCATION_KEY))
                                .details(details)
                                .build());
                    } else if (!(item instanceof Payload)) {
                        throw new IllegalStateException("Item must be a Payload");
                    } else {
                        validators.stream()
                                .filter(v -> v.isApplicable(reference))
                                .forEach(v -> feedbackItems.addAll(v.validateValue(instance, reference, item, currentItemContext)));
                    }
                }
            } else if (value != null && !(value instanceof Collection)) {
                final Map<String, Object> details = new LinkedHashMap<>();
                details.put(Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference));
                if (instance.containsKey(DefaultDispatcher.REFERENCE_ID_KEY)) {
                    details.put(DefaultDispatcher.REFERENCE_ID_KEY, instance.get(DefaultDispatcher.REFERENCE_ID_KEY));
                }
                feedbackItems.add(FeedbackItem.builder()
                        .code("INVALID_CONTENT")
                        .level(FeedbackItem.Level.ERROR)
                        .location(currentContext.get(LOCATION_KEY))
                        .details(details)
                        .build());
            }
        } else {
            if (value instanceof Collection) {
                final Map<String, Object> details = new LinkedHashMap<>();
                details.put(Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference));
                if (instance.containsKey(DefaultDispatcher.REFERENCE_ID_KEY)) {
                    details.put(DefaultDispatcher.REFERENCE_ID_KEY, instance.get(DefaultDispatcher.REFERENCE_ID_KEY));
                }
                feedbackItems.add(FeedbackItem.builder()
                        .code("INVALID_CONTENT")
                        .level(FeedbackItem.Level.ERROR)
                        .location(currentContext.get(LOCATION_KEY))
                        .details(details)
                        .build());
            } else if (value != null && !(value instanceof Payload)) {
                throw new IllegalStateException("Item must be a Payload");
            } else if (!ignoreInvalidValues && value != null) {
                validators.stream()
                        .filter(v -> v.isApplicable(reference))
                        .forEach(v -> feedbackItems.addAll(v.validateValue(instance, reference, value, currentContext)));
            }
        }

        final Optional<EReference> mappedReference = asmUtils.getMappedReference(reference);
        final boolean validateForCreate = createReference != null
                ? mappedReference
                .map(mr -> !asmUtils.getMappedReference(createReference)
                        .map(mappedCreateReference -> mappedCreateReference.getEOpposite())
                        .filter(mappedCreateReferenceOpposite -> AsmUtils.equals(mappedCreateReferenceOpposite, mr))
                        .isPresent())
                .orElse(false)
                : false;

        if (reference.isRequired() && (validateMissingFeatures || instance.containsKey(reference.getName())) && (createReference == null || !mappedReference.isPresent() ? AsmUtils.isEmbedded(reference) : validateForCreate) && value == null) {
            final Map<String, Object> details = new LinkedHashMap<>();
            details.put(Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference));
            if (instance.containsKey(DefaultDispatcher.REFERENCE_ID_KEY)) {
                details.put(DefaultDispatcher.REFERENCE_ID_KEY, instance.get(DefaultDispatcher.REFERENCE_ID_KEY));
            }
            feedbackItems.add(FeedbackItem.builder()
                    .code("MISSING_REQUIRED_RELATION")
                    .level(FeedbackItem.Level.ERROR)
                    .location(currentContext.get(LOCATION_KEY))
                    .details(details)
                    .build());
        }

        return feedbackItems;
    }

    private List<FeedbackItem> validateAttribute(final EAttribute attribute, final Payload instance, final Map<String, Object> validationContext) {
        final Object value = instance.get(attribute.getName());

        final boolean validateMissingFeatures = (Boolean) validationContext.getOrDefault(VALIDATE_MISSING_FEATURES_KEY, VALIDATE_MISSING_FEATURES_DEFAULT);

        final List<FeedbackItem> feedbackItems = new ArrayList<>();

        if (attribute.isRequired() && (validateMissingFeatures || instance.containsKey(attribute.getName())) && value == null) {
            final Map<String, Object> details = new LinkedHashMap<>();
            details.put(Validator.FEATURE_KEY, ATTRIBUTE_TO_MODEL_TYPE.apply(attribute));
            if (instance.containsKey(DefaultDispatcher.REFERENCE_ID_KEY)) {
                details.put(DefaultDispatcher.REFERENCE_ID_KEY, instance.get(DefaultDispatcher.REFERENCE_ID_KEY));
            }
            feedbackItems.add(FeedbackItem.builder()
                    .code("MISSING_REQUIRED_ATTRIBUTE")
                    .level(FeedbackItem.Level.ERROR)
                    .location(validationContext.get(LOCATION_KEY))
                    .details(details)
                    .build());
        }
        if (AsmUtils.isString(attribute.getEAttributeType()) && attribute.isRequired() && (validateMissingFeatures || instance.containsKey(attribute.getName()))
                && value != null && RequiredStringValidatorOption.ACCEPT_NON_EMPTY.equals(requiredStringValidatorOption) && ((String) value).isEmpty()) {
            final Map<String, Object> details = new LinkedHashMap<>();
            details.put(Validator.FEATURE_KEY, ATTRIBUTE_TO_MODEL_TYPE.apply(attribute));
            if (instance.containsKey(DefaultDispatcher.REFERENCE_ID_KEY)) {
                details.put(DefaultDispatcher.REFERENCE_ID_KEY, instance.get(DefaultDispatcher.REFERENCE_ID_KEY));
            }
            feedbackItems.add(FeedbackItem.builder()
                    .code("MISSING_REQUIRED_ATTRIBUTE")
                    .level(FeedbackItem.Level.ERROR)
                    .location(validationContext.get(LOCATION_KEY))
                    .details(details)
                    .build());
        }

        if (value != null) {
            validators.stream()
                    .filter(v -> v.isApplicable(attribute))
                    .forEach(v -> feedbackItems.addAll(v.validateValue(instance, attribute, value, validationContext)));
        }

        return feedbackItems;
    }

    private Object convertEnumerationValue(final EDataType dataType, final Object oldValue) {
        final String literal = oldValue.toString();
        return Optional.ofNullable(asmUtils.all(EEnum.class)
                .filter(e -> AsmUtils.equals(e, dataType))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Invalid enumeration type: " + AsmUtils.getClassifierFQName(dataType)))
                .getEEnumLiteral(literal))
                .map(l -> l.getValue())
                .orElseThrow(() -> new IllegalArgumentException("Invalid enumeration literal '" + literal + "' of type: " + AsmUtils.getClassifierFQName(dataType)));
    }

    private Object convertNonEnumerationValue(final EDataType dataType, final Object oldValue) throws InvalidTokenException {
        if (AsmUtils.isByteArray(dataType) && oldValue instanceof String) {
            final Token<DownloadClaim> token = filestoreTokenValidator.parseDownloadToken((String) oldValue);
            final String contextString = (String) token.get(DownloadClaim.CONTEXT);
            if (contextString != null) {
                final Map<String, Object> context = new Gson().fromJson(contextString, Map.class);
                final String attributeName = (String) context.get(GetUploadTokenCall.ATTRIBUTE_KEY);
                if (attributeName != null && !asmUtils.equals(dataType, asmUtils.resolveAttribute(attributeName).map(a -> a.getEAttributeType()).orElse(null))) {
                    throw new InvalidTokenException(null);
                }
            } else {
                // missing context so attribute type cannot be validated
                throw new InvalidTokenException(null);
            }
            return FileType.builder()
                    .id((String) DownloadClaim.FILE_ID.convert(token.get(DownloadClaim.FILE_ID)))
                    .fileName((String) DownloadClaim.FILE_NAME.convert(token.get(DownloadClaim.FILE_NAME)))
                    .size((Long) DownloadClaim.FILE_SIZE.convert(token.get(DownloadClaim.FILE_SIZE)))
                    .mimeType((String) DownloadClaim.FILE_MIME_TYPE.convert(token.get(DownloadClaim.FILE_MIME_TYPE)))
                    .build();
        } else {
            return coercer.coerce(oldValue, dataType.getInstanceClassName());
        }
    }
}
