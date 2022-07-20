package hu.blackbelt.judo.runtime.core.validator;

import com.google.common.collect.ImmutableMap;
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

import static hu.blackbelt.judo.runtime.core.validator.Validator.*;

@Builder
@Slf4j
public class PayloadValidator {
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

        final Payload payload = Payload.asPayload(input);

        try {
            PayloadTraverser.builder()
                    .predicate((reference) -> (Boolean) validationContext.getOrDefault(VALIDATE_FOR_CREATE_OR_UPDATE_KEY, VALIDATE_FOR_CREATE_OR_UPDATE_DEFAULT)
                            ? asmUtils.getMappedReference(reference).filter(mappedReference -> mappedReference.isContainment()).isPresent()
                            : AsmUtils.isEmbedded(reference) && !(Boolean) validationContext.getOrDefault(NO_TRAVERSE_KEY, NO_TRAVERSE_DEFAULT))
                    .processor((instance, ctx) -> processPayload(instance, ctx, feedbackItems, validationContext))
                    .build()
                    .traverse(payload, transferObjectType);
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


    private void processPayload(Payload instance, PayloadTraverser.PayloadTraverserContext ctx, Collection<FeedbackItem> feedbackItems, Map<String, Object> validationContext) {
        {
            final String containerLocation = (String) validationContext.getOrDefault(LOCATION_KEY, "");
            final Map<String, Object> payloadContext = new TreeMap<>(validationContext);
            payloadContext.put(LOCATION_KEY, (containerLocation.isEmpty() ? "" : containerLocation + "/") + ctx.getPathAsString());

            // do not validate referenced elements but containment only
            final boolean validate = !validators.isEmpty() && ctx.getPath().stream().allMatch(e -> e.getReference().isContainment());
            final boolean ignoreInvalidValues = (Boolean) validationContext.getOrDefault(IGNORE_INVALID_VALUES_KEY, IGNORE_INVALID_VALUES_DEFAULT);
            if (validate) {
                ctx.getType().getEAllAttributes().stream().forEach(attribute -> processAttribute(instance, attribute, feedbackItems, payloadContext, ignoreInvalidValues));
                ctx.getType().getEAllReferences().forEach(reference -> processReference(instance, reference, feedbackItems, payloadContext, ignoreInvalidValues));
            }
        }
    }

    private void processReference(final Payload instance, final EReference reference, final Collection<FeedbackItem> feedbackItems, final Map<String, Object> validationContext, final boolean ignoreInvalidValues) {

        final String containerLocation = (String) validationContext.getOrDefault(LOCATION_KEY, "");
        final Map<String, Object> referenceValidationContext = new TreeMap<>(validationContext);
        String referenceLocation = containerLocation + (containerLocation.isEmpty() || containerLocation.endsWith("/") ? "" : ".") + reference.getName();
        referenceValidationContext.put(LOCATION_KEY, referenceLocation);
        feedbackItems.addAll(validateReference(reference, instance, referenceValidationContext, ignoreInvalidValues));
    }

    private void processAttribute(final Payload instance, final EAttribute attribute, final Collection<FeedbackItem> feedbackItems, final Map<String, Object> validationContext, final boolean ignoreInvalidValue) {
        final String containerLocation = (String) validationContext.getOrDefault(LOCATION_KEY, "");
        final Map<String, Object> attributeValidationContext = new TreeMap<>(validationContext);
        attributeValidationContext.put(LOCATION_KEY, containerLocation + (containerLocation.isEmpty() || containerLocation.endsWith("/") ? "" : ".") + attribute.getName());

        if (!ignoreInvalidValue) {
            feedbackItems.addAll(validateAttribute(attribute, instance, attributeValidationContext));
        }
    }

    public List<FeedbackItem> validateReference(final EReference reference, final Payload instance, final Map<String, Object> validationContext, final boolean ignoreInvalidValues) {
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
                    addValidationError(
                            ImmutableMap.of(
                                    Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference),
                                    PayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(PayloadValidator.REFERENCE_ID_KEY)),
                                    SIZE_PARAMETER, size
                            ),
                            currentContext.get(LOCATION_KEY),
                            feedbackItems,
                            ERROR_TOO_FEW_ITEMS
                    );
                } else if (size > reference.getUpperBound() && reference.getUpperBound() != -1) {
                    addValidationError(
                            ImmutableMap.of(
                                    Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference),
                                    PayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(PayloadValidator.REFERENCE_ID_KEY)),
                                    SIZE_PARAMETER, size
                            ),
                            currentContext.get(LOCATION_KEY),
                            feedbackItems,
                            ERROR_TOO_MANY_ITEMS
                    );
                }
                int idx = 0;
                for (@SuppressWarnings("rawtypes")
                     Iterator it = ((Collection) value).iterator(); it.hasNext(); idx++) {
                    final Map<String, Object> currentItemContext = new TreeMap<>(currentContext);
                    currentItemContext.put(LOCATION_KEY, currentContext.get(LOCATION_KEY) + "[" + idx + "]");

                    final Object item = it.next();
                    if (item == null) {
                        addValidationError(
                                ImmutableMap.of(
                                        Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference),
                                        PayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(PayloadValidator.REFERENCE_ID_KEY))
                                ),
                                currentItemContext.get(LOCATION_KEY),
                                feedbackItems,
                                ERROR_NULL_ITEM_IS_NOT_SUPPORTED
                        );
                    } else if (!(item instanceof Payload)) {
                        throw new IllegalStateException("Item must be a Payload");
                    } else {
                        validators.stream()
                                .filter(v -> v.isApplicable(reference))
                                .forEach(v -> feedbackItems.addAll(v.validateValue(instance, reference, item, currentItemContext)));
                    }
                }
            } else if (value != null && !(value instanceof Collection)) {
                addValidationError(
                        ImmutableMap.of(
                                Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference),
                                PayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(PayloadValidator.REFERENCE_ID_KEY))
                        ),
                        currentContext.get(LOCATION_KEY),
                        feedbackItems,
                        ERROR_INVALID_CONTENT
                );
            }
        } else {
            if (value instanceof Collection) {
                addValidationError(
                        ImmutableMap.of(
                                Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference),
                                PayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(PayloadValidator.REFERENCE_ID_KEY))
                        ),
                        currentContext.get(LOCATION_KEY),
                        feedbackItems,
                        ERROR_INVALID_CONTENT
                );
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
                .map(mr -> asmUtils.getMappedReference(createReference)
                        .map(EReference::getEOpposite)
                        .filter(mappedCreateReferenceOpposite -> AsmUtils.equals(mappedCreateReferenceOpposite, mr))
                        .isEmpty())
                .orElse(false)
                : false;

        if (reference.isRequired() && (validateMissingFeatures || instance.containsKey(reference.getName())) && (createReference == null || mappedReference.isEmpty() ? AsmUtils.isEmbedded(reference) : validateForCreate) && value == null) {
            addValidationError(
                    ImmutableMap.of(
                            Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference),
                            PayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(PayloadValidator.REFERENCE_ID_KEY))
                    ),
                    currentContext.get(LOCATION_KEY),
                    feedbackItems,
                    ERROR_MISSING_REQUIRED_RELATION
            );
        }
        return feedbackItems;
    }

    public List<FeedbackItem> validateAttribute(final EAttribute attribute, final Payload instance, final Map<String, Object> validationContext) {
        final Object value = instance.get(attribute.getName());

        final boolean validateMissingFeatures = (Boolean) validationContext.getOrDefault(VALIDATE_MISSING_FEATURES_KEY, VALIDATE_MISSING_FEATURES_DEFAULT);

        final List<FeedbackItem> feedbackItems = new ArrayList<>();

        if (attribute.isRequired() && (validateMissingFeatures || instance.containsKey(attribute.getName())) && value == null) {
            addValidationError(
                    ImmutableMap.of(
                            Validator.FEATURE_KEY, ATTRIBUTE_TO_MODEL_TYPE.apply(attribute),
                            PayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(PayloadValidator.REFERENCE_ID_KEY))
                    ),
                    validationContext.get(LOCATION_KEY),
                    feedbackItems,
                    ERROR_MISSING_REQUIRED_ATTRIBUTE
            );
        }
        if (AsmUtils.isString(attribute.getEAttributeType()) && attribute.isRequired() && (validateMissingFeatures || instance.containsKey(attribute.getName()))
                && value != null && RequiredStringValidatorOption.ACCEPT_NON_EMPTY.equals(requiredStringValidatorOption) && ((String) value).isEmpty()) {
            addValidationError(
                    ImmutableMap.of(
                            Validator.FEATURE_KEY, ATTRIBUTE_TO_MODEL_TYPE.apply(attribute),
                            PayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(PayloadValidator.REFERENCE_ID_KEY))
                    ),
                    validationContext.get(LOCATION_KEY),
                    feedbackItems,
                    ERROR_MISSING_REQUIRED_ATTRIBUTE
            );
        }

        if (value != null) {
            validators.stream()
                    .filter(v -> v.isApplicable(attribute))
                    .forEach(v -> feedbackItems.addAll(v.validateValue(instance, attribute, value, validationContext)));
        }

        return feedbackItems;
    }

}
