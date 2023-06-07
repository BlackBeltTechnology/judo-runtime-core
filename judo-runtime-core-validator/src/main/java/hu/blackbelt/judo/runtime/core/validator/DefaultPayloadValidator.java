package hu.blackbelt.judo.runtime.core.validator;

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
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dao.api.PayloadValidator;
import hu.blackbelt.judo.dao.api.ValidationResult;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.PayloadTraverser;
import hu.blackbelt.judo.runtime.core.exception.ValidationException;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.*;

import java.util.*;
import java.util.function.*;

import static hu.blackbelt.judo.runtime.core.validator.Validator.*;

@Builder
@Slf4j
public class DefaultPayloadValidator implements PayloadValidator {
    public enum RequiredStringValidatorOption {
        ACCEPT_EMPTY, ACCEPT_NON_EMPTY
    }

    @NonNull
    private final AsmUtils asmUtils;

    @NonNull
    private final Coercer coercer;

    private final RequiredStringValidatorOption requiredStringValidatorOption;

    @SuppressWarnings("rawtypes")
    private final IdentifierProvider identifierProvider;

    private final ValidatorProvider validatorProvider;

    public static final String GLOBAL_VALIDATION_CONTEXT = "globalValidationContext";

    public static final String REFERENCE_ID_KEY = "__referenceId";
    public static final String VERSION_KEY = "__version";
    public static final String SIGNED_IDENTIFIER_KEY = "__signedIdentifier";

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

    public List<ValidationResult> validatePayload(final EClass transferObjectType, final Payload input, final Map<String, Object> validationContext, boolean throwValidationException) throws ValidationException {
        final List<ValidationResult> validationResults = new ArrayList<>();

        validationContext.put(GLOBAL_VALIDATION_CONTEXT, new HashMap<String, Object>());

        final Payload payload = Payload.asPayload(input);

        try {
            PayloadTraverser.builder()
                    .predicate((reference) -> (Boolean) validationContext.getOrDefault(VALIDATE_FOR_CREATE_OR_UPDATE_KEY, VALIDATE_FOR_CREATE_OR_UPDATE_DEFAULT)
                            ? asmUtils.getMappedReference(reference).filter(EReference::isContainment).isPresent()
                            : AsmUtils.isEmbedded(reference) && !(Boolean) validationContext.getOrDefault(NO_TRAVERSE_KEY, NO_TRAVERSE_DEFAULT))
                    .processor((instance, ctx) -> processPayload(instance, ctx, validationResults, validationContext))
                    .build()
                    .traverse(payload, transferObjectType);
        } catch (IllegalArgumentException ex) {
            log.debug("Invalid payload", ex);
        }

        if (throwValidationException && !validationResults.isEmpty()) {
            throw new ValidationException("Validation failed", Collections.unmodifiableCollection(validationResults));
        } else {
            validationContext.put(VALIDATION_RESULT_KEY, validationResults);
        }
        return validationResults;
    }

    private void processPayload(Payload instance, PayloadTraverser.PayloadTraverserContext ctx, Collection<ValidationResult> validationResults, Map<String, Object> validationContext) {
        final Map<String, Object> validationResultContext = new TreeMap<>(validationContext);
        final String containerLocation = (String) validationContext.getOrDefault(LOCATION_KEY, "");
        final Map<String, Object> currentContext = new TreeMap<>(validationContext);
        currentContext.put(LOCATION_KEY, (containerLocation.isEmpty() ? "" : containerLocation + "/") + ctx.getPathAsString());
        validationResultContext.put(LOCATION_KEY, currentContext.get(LOCATION_KEY));

        final boolean ignoreInvalidValues = (Boolean) validationContext.getOrDefault(IGNORE_INVALID_VALUES_KEY, IGNORE_INVALID_VALUES_DEFAULT);
        if (!validatorProvider.getValidators().isEmpty()) {
            ctx.getType().getEAllAttributes().forEach(attribute -> processAttribute(instance, attribute, validationResults, validationResultContext, ignoreInvalidValues));
            ctx.getType().getEAllReferences().stream().filter(EReference::isContainment).forEach(reference -> processReference(instance, reference, validationResults, currentContext, ignoreInvalidValues));
        }
    }

    private void processReference(final Payload instance,
                                  final EReference reference,
                                  final Collection<ValidationResult> validationResults,
                                  final Map<String, Object> validationContext,
                                  final boolean ignoreInvalidValues) {

        validationResults.addAll(validateReference(reference, instance, validationContext, ignoreInvalidValues));
    }

    private void processAttribute(final Payload instance, final EAttribute attribute, final Collection<ValidationResult> validationResults, final Map<String, Object> validationContext, final boolean ignoreInvalidValue) {
        final String containerLocation = (String) validationContext.getOrDefault(LOCATION_KEY, "");
        final Map<String, Object> attributeValidationContext = new TreeMap<>(validationContext);
        attributeValidationContext.put(LOCATION_KEY, containerLocation + (containerLocation.isEmpty() || containerLocation.endsWith("/") ? "" : ".") + attribute.getName());

        if (!ignoreInvalidValue) {
            validationResults.addAll(validateAttribute(attribute, instance, attributeValidationContext));
        }
    }

    public List<ValidationResult> validateReference(final EReference reference, final Payload instance, final Map<String, Object> validationContext, final boolean ignoreInvalidValues) {
        final Object value = instance.get(reference.getName());
        final List<ValidationResult> validationResults = new ArrayList<>();

        final String containerLocation = (String) validationContext.getOrDefault(LOCATION_KEY, "");
        final Map<String, Object> currentContext = new TreeMap<>(validationContext);
        currentContext.put(LOCATION_KEY, containerLocation + (containerLocation.isEmpty() || containerLocation.endsWith("/") ? "" : ".") + reference.getName());
        final EReference createReference = (EReference) validationContext.get(CREATE_REFERENCE_KEY);
        boolean validateMissingFeatures = (Boolean) validationContext.getOrDefault(VALIDATE_MISSING_FEATURES_KEY, VALIDATE_MISSING_FEATURES_DEFAULT);

        if (validateMissingFeatures && getIdentifier(instance) == null &&
                reference.isChangeable() && AsmUtils.getExtensionAnnotationValue(reference, "default", false).isPresent()) {
            validateMissingFeatures = false;
        }

        if (reference.isMany()) {
            if (!ignoreInvalidValues && value instanceof Collection) {
                @SuppressWarnings("rawtypes")
                final int size = ((Collection) value).size();
                if (size < reference.getLowerBound()) {
                    addValidationError(
                            ImmutableMap.of(
                                    Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference),
                                    DefaultPayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(DefaultPayloadValidator.REFERENCE_ID_KEY)),
                                    SIZE_PARAMETER, size
                            ),
                            currentContext.get(LOCATION_KEY),
                            validationResults,
                            ERROR_TOO_FEW_ITEMS
                    );
                } else if (size > reference.getUpperBound() && reference.getUpperBound() != -1) {
                    addValidationError(
                            ImmutableMap.of(
                                    Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference),
                                    DefaultPayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(DefaultPayloadValidator.REFERENCE_ID_KEY)),
                                    SIZE_PARAMETER, size
                            ),
                            currentContext.get(LOCATION_KEY),
                            validationResults,
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
                                        DefaultPayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(DefaultPayloadValidator.REFERENCE_ID_KEY))
                                ),
                                currentItemContext.get(LOCATION_KEY),
                                validationResults,
                                ERROR_NULL_ITEM_IS_NOT_SUPPORTED
                        );
                    } else if (!(item instanceof Payload)) {
                        throw new IllegalStateException("Item must be a Payload");
                    } else {
                        validatorProvider.getValidators().stream()
                                .filter(v -> v.isApplicable(reference))
                                .forEach(v -> validationResults.addAll(v.validateValue(instance, reference, item, currentItemContext)));
                    }
                }
            } else if (value != null && !(value instanceof Collection)) {
                addValidationError(
                        ImmutableMap.of(
                                Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference),
                                DefaultPayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(DefaultPayloadValidator.REFERENCE_ID_KEY))
                        ),
                        currentContext.get(LOCATION_KEY),
                        validationResults,
                        ERROR_INVALID_CONTENT
                );
            }
        } else {
            if (value instanceof Collection) {
                addValidationError(
                        ImmutableMap.of(
                                Validator.FEATURE_KEY, REFERENCE_TO_MODEL_TYPE.apply(reference),
                                DefaultPayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(DefaultPayloadValidator.REFERENCE_ID_KEY))
                        ),
                        currentContext.get(LOCATION_KEY),
                        validationResults,
                        ERROR_INVALID_CONTENT
                );
            } else if (value != null && !(value instanceof Payload)) {
                throw new IllegalStateException("Item must be a Payload");
            } else if (!ignoreInvalidValues && value != null) {
                validatorProvider.getValidators().stream()
                        .filter(v -> v.isApplicable(reference))
                        .forEach(v -> validationResults.addAll(v.validateValue(instance, reference, value, currentContext)));
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
                            DefaultPayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(DefaultPayloadValidator.REFERENCE_ID_KEY))
                    ),
                    currentContext.get(LOCATION_KEY),
                    validationResults,
                    ERROR_MISSING_REQUIRED_RELATION
            );
        }
        return validationResults;
    }

    public Collection<ValidationResult> validateAttribute(final EAttribute attribute, final Payload instance, final Map<String, Object> validationContext) {
        final Object value = instance.get(attribute.getName());
        boolean validateMissingFeatures = (Boolean) validationContext.getOrDefault(VALIDATE_MISSING_FEATURES_KEY, VALIDATE_MISSING_FEATURES_DEFAULT);

        if (validateMissingFeatures &&
            (getIdentifier(instance) == null || !instance.containsKey(attribute.getName())) &&
            attribute.isChangeable() &&
            AsmUtils.getExtensionAnnotationValue(attribute, "default", false).isPresent()) {

            validateMissingFeatures = false;
        }

        final List<ValidationResult> validationResults = new ArrayList<>();

        if ((attribute.isRequired() || asmUtils.getMappedAttribute(attribute).map(EAttribute::isRequired).orElse(false)) &&
            (validateMissingFeatures || instance.containsKey(attribute.getName())) &&
            value == null) {

            addValidationError(
                    ImmutableMap.of(
                            Validator.FEATURE_KEY, ATTRIBUTE_TO_MODEL_TYPE.apply(attribute),
                            DefaultPayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(DefaultPayloadValidator.REFERENCE_ID_KEY))
                    ),
                    validationContext.get(LOCATION_KEY),
                    validationResults,
                    ERROR_MISSING_REQUIRED_ATTRIBUTE
            );
        }
        if (AsmUtils.isString(attribute.getEAttributeType()) &&
            (attribute.isRequired() || asmUtils.getMappedAttribute(attribute).map(EAttribute::isRequired).orElse(false)) &&
            (validateMissingFeatures || instance.containsKey(attribute.getName())) &&
            value != null &&
            RequiredStringValidatorOption.ACCEPT_NON_EMPTY.equals(requiredStringValidatorOption) &&
            ((String) value).isEmpty()) {

            addValidationError(
                    ImmutableMap.of(
                            Validator.FEATURE_KEY, ATTRIBUTE_TO_MODEL_TYPE.apply(attribute),
                            DefaultPayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(DefaultPayloadValidator.REFERENCE_ID_KEY))
                    ),
                    validationContext.get(LOCATION_KEY),
                    validationResults,
                    ERROR_MISSING_REQUIRED_ATTRIBUTE
            );
        }

        if (value != null) {
            validatorProvider.getValidators().stream()
                    .filter(v -> v.isApplicable(attribute))
                    .forEach(v -> validationResults.addAll(v.validateValue(instance, attribute, value, validationContext)));
        }

        return validationResults;
    }


    private Object getIdentifier(Payload instance) {
        Object identifier = null;
        if (identifierProvider != null) {
            identifier = instance.get(identifierProvider.getName());
        }
        return identifier;
    }
}
