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
import com.google.gson.Gson;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dao.api.PayloadValidator;
import hu.blackbelt.judo.dao.api.ValidationResult;
import hu.blackbelt.judo.dispatcher.api.FileType;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.PayloadTraverser;
import hu.blackbelt.judo.runtime.core.exception.ValidationException;
import hu.blackbelt.judo.runtime.core.dispatcher.behaviours.GetUploadTokenCall;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;
import hu.blackbelt.judo.runtime.core.validator.DefaultPayloadValidator;
import hu.blackbelt.judo.runtime.core.validator.Validator;
import hu.blackbelt.judo.runtime.core.validator.ValidatorProvider;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.osgi.filestore.security.api.DownloadClaim;
import hu.blackbelt.osgi.filestore.security.api.Token;
import hu.blackbelt.osgi.filestore.security.api.TokenValidator;
import hu.blackbelt.osgi.filestore.security.api.exceptions.InvalidTokenException;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.*;

import java.util.*;
import java.util.function.Function;

import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.*;
import static hu.blackbelt.judo.runtime.core.validator.DefaultPayloadValidator.LOCATION_KEY;
import static hu.blackbelt.judo.runtime.core.validator.DefaultPayloadValidator.VALIDATION_RESULT_KEY;
import static hu.blackbelt.judo.runtime.core.validator.Validator.*;

@Slf4j
public class RequestConverter {

    private final EClass transferObjectType;

    private final Coercer coercer;

    private final PayloadValidator payloadValidator;

    private final TokenValidator filestoreTokenValidator;

    private final boolean trimString;

    private final IdentifierSigner identifierSigner;

    @SuppressWarnings("rawtypes")
    private final IdentifierProvider identifierProvider;

    private final ValidatorProvider validatorProvider;

    private final boolean throwValidationException;

    @NonNull
    @Singular
    private final Collection<String> keepProperties;

    private final AsmUtils asmUtils;

    public static final String NO_TRAVERSE_KEY = "noTraverse";
    public static final String VALIDATE_FOR_CREATE_OR_UPDATE_KEY = "validateForCreateOrUpdate";
    public static final String VALIDATE_MISSING_FEATURES_KEY = "validateMissingFeatures";
    public static final String IGNORE_INVALID_VALUES_KEY = "ignoreInvalidValues";

    public static final String VALIDATE_ROOT_MISSING_FEATURES_KEY = "validateRootMissingFeatures";

    public static final String IS_ROOT_KEY = "isRoot";

    private static final boolean VALIDATE_FOR_CREATE_OR_UPDATE_DEFAULT = false;
    private static final boolean NO_TRAVERSE_DEFAULT = false;
    private static final boolean VALIDATE_MISSING_FEATURES_DEFAULT = true;
    private static final boolean IGNORE_INVALID_VALUES_DEFAULT = false;

    private static final boolean VALIDATE_ROOT_MISSING_FEATURES_DEFAULT = true;

    public static final String GLOBAL_VALIDATION_CONTEXT = "globalValidationContext";

    public static final Function<EAttribute, String> ATTRIBUTE_TO_MODEL_TYPE = attribute -> getAttributeFQName(attribute).replaceAll("\\W", "_");
    public static final Function<EReference, String> REFERENCE_TO_MODEL_TYPE = reference -> getReferenceFQName(reference).replaceAll("\\W", "_");

    @Builder
    public RequestConverter(@NonNull EClass transferObjectType,
                            @NonNull AsmModel asmModel,
                            @NonNull Coercer coercer,
                            @NonNull PayloadValidator payloadValidator,
                            @NonNull @Singular Collection<String> keepProperties,
                            TokenValidator filestoreTokenValidator,
                            boolean trimString,
                            IdentifierSigner identifierSigner,
                            @SuppressWarnings("rawtypes")
                            IdentifierProvider identifierProvider,
                            ValidatorProvider validatorProvider,
                            boolean throwValidationException) {
        this.transferObjectType = transferObjectType;
        this.coercer = coercer;
        this.payloadValidator = payloadValidator;
        this.filestoreTokenValidator = filestoreTokenValidator;
        this.trimString = trimString;
        this.identifierProvider = identifierProvider;
        this.validatorProvider = validatorProvider;
        this.throwValidationException = throwValidationException;
        this.keepProperties = keepProperties;
        this.identifierSigner = identifierSigner;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());

    }

    public Optional<Payload> convert(final Map<String, Object> input, final Map<String, Object> validationContext) throws ValidationException {
        if (input == null) {
            return Optional.empty();
        }

        final Map<String, Object> rootContext = new TreeMap<>(validationContext);

        rootContext.put(GLOBAL_VALIDATION_CONTEXT, new HashMap<String, Object>());

        final List<ValidationResult> validationResults = new ArrayList<>(validateIdentifier(transferObjectType, input, rootContext));
        final Payload payload = Payload.asPayload(input);

        try {
            PayloadTraverser.builder()
                    .predicate((reference) -> (Boolean) validationContext.getOrDefault(VALIDATE_FOR_CREATE_OR_UPDATE_KEY, VALIDATE_FOR_CREATE_OR_UPDATE_DEFAULT)
                            ? asmUtils.getMappedReference(reference).map(e -> !e.isDerived()).orElse(false)
                            : !(Boolean) validationContext.getOrDefault(NO_TRAVERSE_KEY, NO_TRAVERSE_DEFAULT))
                    .processor((instance, ctx) -> processPayload(instance, ctx, validationResults, validationContext, rootContext))
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

        return Optional.of(payload);
    }

    private void processPayload(Payload instance, PayloadTraverser.PayloadTraverserContext ctx, Collection<ValidationResult> validationResults, Map<String, Object> validationContext, Map<String, Object> feedbackContext) {

        final String containerLocation = (String) validationContext.getOrDefault(LOCATION_KEY, "");
        final Map<String, Object> currentContext = new TreeMap<>(validationContext);
        currentContext.put(LOCATION_KEY, (containerLocation.isEmpty() ? "" : containerLocation + "/") + ctx.getPathAsString());
        currentContext.put(IS_ROOT_KEY, ctx.getPathAsString().isEmpty());
        feedbackContext.put(LOCATION_KEY, currentContext.get(LOCATION_KEY));
        feedbackContext.put(IS_ROOT_KEY, currentContext.get(IS_ROOT_KEY));

        // Validate only elements which is contained only from root payload
        final boolean validate = !validatorProvider.getValidators().isEmpty() && ctx.getPath().stream().allMatch(e -> e.getReference().isContainment());

        final boolean ignoreInvalidValues = (Boolean) validationContext.getOrDefault(IGNORE_INVALID_VALUES_KEY, IGNORE_INVALID_VALUES_DEFAULT);
        ctx.getType().getEAllAttributes().forEach(a -> processAttribute(instance, a, validationResults, validate, feedbackContext, ignoreInvalidValues));

        if ((Boolean) validationContext.getOrDefault(VALIDATE_FOR_CREATE_OR_UPDATE_KEY, VALIDATE_FOR_CREATE_OR_UPDATE_DEFAULT) && identifierProvider != null) {
            ctx.getType().getEAllReferences().stream()
                    .filter(r -> isEmbedded(r) && !AsmUtils.isAllowedToCreateEmbeddedObject(r) && asmUtils.getMappedReference(r).filter(EReference::isContainment).isPresent())
                    .forEach(c -> removeNonCreatableReferenceElements(instance, c));
        }
        if (validate) {
            ctx.getType().getEAllReferences().forEach(r -> processReference(instance, r, validationResults, currentContext, feedbackContext, ignoreInvalidValues));
        }
        instance.entrySet().removeIf(entry -> ctx.getType().getEAllStructuralFeatures().stream().noneMatch(f -> Objects.equals(f.getName(), entry.getKey()))
                && !keepProperties.contains(entry.getKey()));
    }

    private void removeNonCreatableReferenceElements(Payload instance, EReference reference) {
        if (reference.isMany()) {
            final Collection<Payload> containments = instance.getAsCollectionPayload(reference.getName());
            if (containments != null) {
                for (final Iterator<Payload> it = containments.iterator(); it.hasNext(); ) {
                    final Payload p = it.next();
                    if (p == null || p.get(identifierProvider.getName()) == null && p.get(IdentifierSigner.SIGNED_IDENTIFIER_KEY) == null) {
                        it.remove();
                        log.debug("Removed non-createable, not existing reference: {}, instance: {}", getReferenceFQName(reference), p);
                    }
                }
            }
        } else {
            final Payload containment = instance.getAsPayload(reference.getName());
            if (containment != null && containment.get(identifierProvider.getName()) == null && containment.get(IdentifierSigner.SIGNED_IDENTIFIER_KEY) == null) {
                instance.remove(reference.getName());
                log.debug("Removed non-createable, not existing reference: {}, instance: {}", getReferenceFQName(reference), containment);
            }
        }
    }

    private void processReference(Payload instance, EReference reference, Collection<ValidationResult> validationResults, Map<String, Object> currentContext, Map<String, Object> feedbackContext, boolean ignoreInvalidValues) {

        validateReferencedIdentifiers(instance, reference, validationResults, currentContext, feedbackContext);
        validationResults.addAll(payloadValidator.validateReference(reference, instance, currentContext, ignoreInvalidValues));
    }

    private void validateReferencedIdentifiers(Payload instance, EReference reference, Collection<ValidationResult> validationResults, Map<String, Object> validationContext, Map<String, Object> feedbackContext) {
        final String referenceLocation = validationContext.get(LOCATION_KEY) + (((String) validationContext.get(LOCATION_KEY)).isEmpty() || ((String) validationContext.get(LOCATION_KEY)).endsWith("/") ? "" : ".");
        final Map<String, Object> referenceValidationContext = new TreeMap<>(feedbackContext);
        referenceValidationContext.put(LOCATION_KEY, referenceLocation);

        if (instance.get(reference.getName()) instanceof Payload && !reference.isMany()) {
            referenceValidationContext.put(LOCATION_KEY, referenceLocation + reference.getName());
            validationResults.addAll(validateIdentifier(reference.getEReferenceType(), instance.getAsPayload(reference.getName()), referenceValidationContext));
        } else if (instance.get(reference.getName()) instanceof Collection && reference.isMany()) {
            int idx = 0;
            for (Iterator<Payload> it = instance.getAsCollectionPayload(reference.getName()).iterator(); it.hasNext(); idx++) {
                referenceValidationContext.put(LOCATION_KEY, referenceLocation + reference.getName() + "[" + idx + "]");
                validationResults.addAll(validateIdentifier(reference.getEReferenceType(), it.next(), referenceValidationContext));
            }
        }
    }


    private List<ValidationResult> validateIdentifier(final EClass clazz, final Map<String, Object> instance, final Map<String, Object> validationContext) {
        if (instance == null) {
            return Collections.emptyList();
        }

        final List<ValidationResult> validationResults = new ArrayList<>();

        if (identifierSigner != null) {
            try {
                identifierSigner.extractSignedIdentifier(clazz, instance);
            } catch (RuntimeException ex) {
                log.info("Invalid signature: {}", ex.getMessage());
                if (log.isDebugEnabled()) {
                    log.debug("Extracting signature failed", ex);
                }
                addValidationError(ImmutableMap.of(
                        DefaultDispatcher.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(DefaultDispatcher.REFERENCE_ID_KEY))),
                        validationContext.get(LOCATION_KEY),
                        validationResults,
                        ERROR_INVALID_IDENTIFIER
                );

            }
        }
        return validationResults;
    }

    private void processAttribute(final Payload instance, final EAttribute attribute, Collection<ValidationResult> validationResults, final boolean validate, final Map<String, Object> validationContext, final boolean ignoreInvalidValue) {
        final Object value = instance.get(attribute.getName());

        final String containerLocation = (String) validationContext.getOrDefault(LOCATION_KEY, "");
        final Map<String, Object> attributeContext = new TreeMap<>(validationContext);
        attributeContext.put(LOCATION_KEY, containerLocation + (containerLocation.isEmpty() || containerLocation.endsWith("/") ? "" : ".") + attribute.getName());

        Object converted = null;
        try {
            if (value != null) {
                if (attribute.getEAttributeType() instanceof EEnum) {
                    converted = convertEnumerationValue(attribute.getEAttributeType(), value);
                } else if (AsmUtils.isByteArray(attribute.getEAttributeType()) && value instanceof String) {
                    converted = convertBinaryValue(attribute.getEAttributeType(), value);
                } else {
                    converted =  coercer.coerce(value, attribute.getEAttributeType().getInstanceClassName());
                }
                if (trimString && converted instanceof String && AsmUtils.isString(attribute.getEAttributeType())) {
                    converted = ((String) converted).trim();
                }
            }
        } catch (InvalidTokenException ex) {
            addValidationError(
                    ImmutableMap.of(
                            Validator.FEATURE_KEY, ATTRIBUTE_TO_MODEL_TYPE.apply(attribute),
                            DefaultPayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(DefaultPayloadValidator.REFERENCE_ID_KEY)),
                            Validator.VALUE_KEY, value
                    ),
                    attributeContext.get(LOCATION_KEY),
                    validationResults,
                    ERROR_INVALID_FILE_TOKEN);
        } catch (RuntimeException ex) {
            if (!ignoreInvalidValue) {
                if (!validate) {
                    throw ex;
                } else {
                    addValidationError(
                            ImmutableMap.of(
                                    Validator.FEATURE_KEY, ATTRIBUTE_TO_MODEL_TYPE.apply(attribute),
                                    DefaultPayloadValidator.REFERENCE_ID_KEY, Optional.ofNullable(instance.get(DefaultPayloadValidator.REFERENCE_ID_KEY)),
                                    Validator.VALUE_KEY, value
                            ),
                            attributeContext.get(LOCATION_KEY),
                            validationResults,
                            ERROR_CONVERSION_FAILED
                    );
                }
            } else {
                log.debug("Ignored invalid value of {}: {}", new Object[] {getAttributeFQName(attribute), value}, ex);
            }
        }

        if (instance.containsKey(attribute.getName())) {
            instance.put(attribute.getName(), converted);
        }

        if (validate && !ignoreInvalidValue) {
            validationResults.addAll(payloadValidator.validateAttribute(attribute, instance, attributeContext));
        }
    }

    private Object convertEnumerationValue(final EDataType dataType, final Object oldValue) {
        final String literal = oldValue.toString();
        return Optional.ofNullable(asmUtils.all(EEnum.class)
                .filter(e -> AsmUtils.equals(e, dataType))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Invalid enumeration type: " + AsmUtils.getClassifierFQName(dataType)))
                .getEEnumLiteral(literal))
                .map(EEnumLiteral::getValue)
                .orElseThrow(() -> new IllegalArgumentException("Invalid enumeration literal '" + literal + "' of type: " + AsmUtils.getClassifierFQName(dataType)));
    }

    private Object convertBinaryValue(final EDataType dataType, final Object oldValue) throws InvalidTokenException {
        final Token<DownloadClaim> token = filestoreTokenValidator.parseDownloadToken((String) oldValue);
        final String contextString = (String) token.get(DownloadClaim.CONTEXT);
        if (contextString != null) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> context = new Gson().fromJson(contextString, Map.class);
            final String attributeName = (String) context.get(GetUploadTokenCall.ATTRIBUTE_KEY);
            if (attributeName != null && !AsmUtils.equals(dataType, asmUtils.resolveAttribute(attributeName).map(EAttribute::getEAttributeType).orElse(null))) {
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
    }

}
