package hu.blackbelt.judo.runtime.core.validator;

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.PayloadTraverser;
import hu.blackbelt.judo.runtime.core.exception.FeedbackItem;
import hu.blackbelt.judo.runtime.core.exception.ValidationException;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.*;

import java.util.*;
import java.util.function.Function;

@Builder
@Slf4j
public class DaoPayloadValidator {
    public enum RequiredStringValidatorOption {
        ACCEPT_EMPTY, ACCEPT_NON_EMPTY
    }

    @NonNull
    private final AsmUtils asmUtils;

    @NonNull
    private final Coercer coercer;

    private final boolean trimString;

    private final RequiredStringValidatorOption requiredStringValidatorOption;

    @SuppressWarnings("rawtypes")
    private final IdentifierProvider identifierProvider;

    private final Collection<Validator> validators;

    private final boolean throwValidationException;

    public static final String REFERENCE_ID_KEY = "__referenceId";
    public static final String VERSION_KEY = "__version";

    public static final String VALIDATION_RESULT_KEY = "validationResult";
    public static final String LOCATION_KEY = "location";
    public static final String CREATE_REFERENCE_KEY = "createReference";
    public static final String NO_TRAVERSE_KEY = "noTraverse";
    public static final String VALIDATE_FOR_CREATE_OR_UPDATE_KEY = "validateForCreateOrUpdate";
    public static final String VALIDATE_MISSING_FEATURES_KEY = "validateMissingFeatures";
    public static final String IGNORE_INVALID_VALUES_KEY = "ignoreInvalidValues";

    private static final boolean VALIDATE_FOR_CREATE_OR_UPDATE_DEFAULT = false;
    private static final boolean NO_TRAVERSE_DEFAULT = false;
    private static final boolean VALIDATE_MISSING_FEATURES_DEFAULT = true;
    private static final boolean IGNORE_INVALID_VALUES_DEFAULT = false;

    public static final Function<EAttribute, String> ATTRIBUTE_TO_MODEL_TYPE = attribute -> AsmUtils.getAttributeFQName(attribute).replaceAll("[^a-zA-Z0-9_]", "_");
    public static final Function<EReference, String> REFERENCE_TO_MODEL_TYPE = reference -> AsmUtils.getReferenceFQName(reference).replaceAll("[^a-zA-Z0-9_]", "_");

    /*
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

     */

    public List<FeedbackItem> validateInsert(final EClass transferObjectType, Payload input) throws ValidationException {
        final Map<String, Object> validationContext = new TreeMap<>();
        validationContext.put(LOCATION_KEY, "");
        validationContext.put(VALIDATE_FOR_CREATE_OR_UPDATE_KEY, true);
        validationContext.put(CREATE_REFERENCE_KEY, null);
        return validatePayload(transferObjectType, input, validationContext);
    }

    public List<FeedbackItem> validateUpdate(final EClass transferObjectType, Payload input) throws ValidationException {
        final Map<String, Object> validationContext = new TreeMap<>();
        validationContext.put(LOCATION_KEY, "");
        validationContext.put(VALIDATE_FOR_CREATE_OR_UPDATE_KEY, true);
        validationContext.put(CREATE_REFERENCE_KEY, null);
        return validatePayload(transferObjectType, input, validationContext);
    }

    public List<FeedbackItem> validateSetReference(final EClass transferObjectType, Payload input) throws ValidationException {
        final Map<String, Object> validationContext = new TreeMap<>();
        validationContext.put(NO_TRAVERSE_KEY, true);
        validationContext.put(VALIDATE_MISSING_FEATURES_KEY, false);
        return validatePayload(transferObjectType, input, validationContext);
    }

    public List<FeedbackItem> validateUnsetReference(final EClass transferObjectType, Payload input) throws ValidationException {
        final Map<String, Object> validationContext = new TreeMap<>();
        validationContext.put(NO_TRAVERSE_KEY, true);
        validationContext.put(VALIDATE_MISSING_FEATURES_KEY, false);
        return validatePayload(transferObjectType, input, validationContext);
    }

    public List<FeedbackItem> validateAddReference(final EClass transferObjectType, Payload input) throws ValidationException {
        final Map<String, Object> validationContext = new TreeMap<>();
        validationContext.put(NO_TRAVERSE_KEY, true);
        validationContext.put(VALIDATE_MISSING_FEATURES_KEY, false);
        return validatePayload(transferObjectType, input, validationContext);
    }

    public List<FeedbackItem> validateRemoveReference(final EClass transferObjectType, Payload input) throws ValidationException {
        final Map<String, Object> validationContext = new TreeMap<>();
        validationContext.put(NO_TRAVERSE_KEY, true);
        validationContext.put(VALIDATE_MISSING_FEATURES_KEY, false);
        return validatePayload(transferObjectType, input, validationContext);
    }

    private List<FeedbackItem> validatePayload(final EClass transferObjectType, final Payload input, final Map<String, Object> validationContext) throws ValidationException {
        final List<FeedbackItem> feedbackItems = new ArrayList<>();

        final Map<String, Object> lastContext = new TreeMap<>(validationContext);

        try {
            PayloadTraverser.builder()
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
                        ctx.getType().getEAllAttributes().stream().forEach(a -> feedbackItems.addAll(validateAttribute(a, instance, validate, currentContext, ignoreInvalidValues)));
                        if (validate) {
                            ctx.getType().getEAllReferences().forEach(r -> {
                                final String referenceLocation = currentContext.get(LOCATION_KEY) + (((String) currentContext.get(LOCATION_KEY)).isEmpty() || ((String) currentContext.get(LOCATION_KEY)).endsWith("/") ? "" : ".");
                                final Map<String, Object> referenceValidationContext = new TreeMap<>(lastContext);

                                if (instance.get(r.getName()) instanceof Payload && !r.isMany()) {
                                    referenceValidationContext.put(LOCATION_KEY, referenceLocation + r.getName());
                                } else if (instance.get(r.getName()) instanceof Collection && r.isMany()) {
                                    int idx = 0;
                                    for (Iterator<Payload> it = instance.getAsCollectionPayload(r.getName()).iterator(); it.hasNext(); idx++) {
                                        referenceValidationContext.put(LOCATION_KEY, referenceLocation + r.getName() + "[" + idx + "]");
                                    }
                                }
                                feedbackItems.addAll(validateReference(r, instance, currentContext, ignoreInvalidValues));
                            });
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
        return feedbackItems;
    }

    private List<FeedbackItem> validateAttribute(final EAttribute attribute, final Payload instance, final boolean validate, final Map<String, Object> validationContext, final boolean ignoreInvalidValue) {
        final List<FeedbackItem> feedbackItems = new ArrayList<>();

        final String containerLocation = (String) validationContext.getOrDefault(LOCATION_KEY, "");
        final Map<String, Object> currentContext = new TreeMap<>(validationContext);
        currentContext.put(LOCATION_KEY, containerLocation + (containerLocation.isEmpty() || containerLocation.endsWith("/") ? "" : ".") + attribute.getName());

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
                @SuppressWarnings("rawtypes")
                final int size = ((Collection) value).size();
                if (size < reference.getLowerBound()) {
                    final Map<String, Object> details = new LinkedHashMap<>();
                    details.put(Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference));
                    details.put("size", size);
                    addReferenceKeyToDetails(instance, details);
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
                    addReferenceKeyToDetails(instance, details);
                    feedbackItems.add(FeedbackItem.builder()
                            .code("TOO_MANY_ITEMS")
                            .level(FeedbackItem.Level.ERROR)
                            .location(currentContext.get(LOCATION_KEY))
                            .details(details)
                            .build());
                }
                int idx = 0;
                for (@SuppressWarnings("rawtypes")
                     Iterator it = ((Collection) value).iterator(); it.hasNext(); idx++) {
                    final Map<String, Object> currentItemContext = new TreeMap<>(currentContext);
                    currentItemContext.put(LOCATION_KEY, currentContext.get(LOCATION_KEY) + "[" + idx + "]");

                    final Object item = it.next();
                    if (item == null) {
                        final Map<String, Object> details = new LinkedHashMap<>();
                        details.put(Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference));
                        addReferenceKeyToDetails(instance, details);
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
                addReferenceKeyToDetails(instance, details);
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
                addReferenceKeyToDetails(instance, details);
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
            addReferenceKeyToDetails(instance, details);
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
            addReferenceKeyToDetails(instance, details);
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
            addReferenceKeyToDetails(instance, details);
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


    private void addReferenceKeyToDetails(Payload instance, Map<String, Object> details) {
        if (instance.containsKey(REFERENCE_ID_KEY)) {
            details.put(REFERENCE_ID_KEY, instance.get(REFERENCE_ID_KEY));
        }
    }
}
