package hu.blackbelt.judo.runtime.core.validator;

import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dao.api.ValidationResult;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;

import java.util.*;
import java.util.stream.Collectors;

import static hu.blackbelt.judo.runtime.core.validator.DefaultPayloadValidator.*;

@RequiredArgsConstructor
@Slf4j
public class RangeValidator<ID> implements Validator {

    private static final String CONSTRAINT_NAME = "range";

    @NonNull
    private final DAO<ID> dao;

    @NonNull
    private final IdentifierProvider<ID> identifierProvider;

    @NonNull
    Context context;

    @Override
    public boolean isApplicable(final EStructuralFeature feature) {
        return feature instanceof EReference && AsmUtils.isEmbedded((EReference) feature) &&
                !AsmUtils.getExtensionAnnotationListByName(feature, CONSTRAINT_NAME).isEmpty();
    }

    @Override
    public Collection<ValidationResult> validateValue(Payload instance, final EStructuralFeature feature, final Object value, final Map<String, Object> validationContext) {
        final Collection<ValidationResult> validationResults = new ArrayList<>();

        if (!AsmUtils.getExtensionAnnotationValue(feature, "range", false).isPresent()) {
            return validationResults;
        }

        @SuppressWarnings("unchecked")
		final Collection<Payload> range = dao.getRangeOf((EReference) feature, instance, DAO.QueryCustomizer.<ID>builder()
                .withoutFeatures(true)
                .build());

        final Collection<ID> validIds = range.stream()
                .map(ri -> ri.getAs(identifierProvider.getType(), identifierProvider.getName()))
                .collect(Collectors.toSet());

        if (validIds.isEmpty()) {
            log.warn("Range of {} contains no items", AsmUtils.getReferenceFQName((EReference) feature));
        }

        final ID id = ((Payload) value).getAs(identifierProvider.getType(), identifierProvider.getName());
        if (id == null || !validIds.contains(id)) {
            Validator.addValidationError(ImmutableMap.of(
                            identifierProvider.getName(), id,
                            SIGNED_IDENTIFIER_KEY, Optional.ofNullable(((Payload) value).get(SIGNED_IDENTIFIER_KEY)),
                            REFERENCE_ID_KEY, Optional.ofNullable(instance.get(REFERENCE_ID_KEY))
                    ),
                    validationContext.get(LOCATION_KEY),
                    validationResults,
                    ERROR_NOT_ACCEPTED_BY_RANGE);
        }

        return validationResults;
    }
}
