package hu.blackbelt.judo.runtime.core.dispatcher.validators;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.runtime.core.exception.FeedbackItem;
import hu.blackbelt.judo.runtime.core.dispatcher.DefaultDispatcher;
import hu.blackbelt.judo.runtime.core.dispatcher.RequestConverter;
import hu.blackbelt.judo.runtime.core.dispatcher.behaviours.AlwaysRollbackTransactionalBehaviourCall;
import hu.blackbelt.judo.runtime.core.dispatcher.behaviours.BehaviourCall;
import hu.blackbelt.judo.runtime.core.dispatcher.security.IdentifierSigner;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;

import javax.transaction.TransactionManager;
import java.util.*;
import java.util.stream.Collectors;

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

    @NonNull
    TransactionManager transactionManager;

    @Override
    public boolean isApplicable(final EStructuralFeature feature) {
        return feature instanceof EReference && AsmUtils.isEmbedded((EReference) feature) &&
                !AsmUtils.getExtensionAnnotationListByName(feature, CONSTRAINT_NAME).isEmpty();
    }

    @Override
    public Collection<FeedbackItem> validateValue(Payload instance, final EStructuralFeature feature, final Object value, final Map<String, Object> validationContext) {
        final Collection<FeedbackItem> feedbackItems = new ArrayList<>();

        if (!AsmUtils.getExtensionAnnotationValue(feature, "range", false).isPresent()) {
            return feedbackItems;
        }

        final BehaviourCall getRangeCall = new AlwaysRollbackTransactionalBehaviourCall(context, transactionManager) {
            @Override
            public Object callInRollbackTransaction(Map<String, Object> exchange, EOperation operation) {
                return dao.getRangeOf((EReference) feature, instance, DAO.QueryCustomizer.<ID>builder()
                        .withoutFeatures(true)
                        .build());
            }

            @Override
            public boolean isSuitableForOperation(EOperation operation) {
                return false;
            }
        };

        final Collection<Payload> range = (List<Payload>) getRangeCall.call(null, null);
        final Collection<ID> validIds = range.stream()
                .map(ri -> ri.getAs(identifierProvider.getType(), identifierProvider.getName()))
                .collect(Collectors.toSet());

        if (validIds.isEmpty()) {
            log.warn("Range of {} contains no items", AsmUtils.getReferenceFQName((EReference) feature));
        }

        final ID id = ((Payload) value).getAs(identifierProvider.getType(), identifierProvider.getName());
        if (id == null || !validIds.contains(id)) {
            final Map<String, Object> details = new LinkedHashMap<>();
            details.put(identifierProvider.getName(), id);
            details.put(IdentifierSigner.SIGNED_IDENTIFIER_KEY, ((Payload) value).get(IdentifierSigner.SIGNED_IDENTIFIER_KEY));
            if (instance.containsKey(DefaultDispatcher.REFERENCE_ID_KEY)) {
                details.put(DefaultDispatcher.REFERENCE_ID_KEY, instance.get(DefaultDispatcher.REFERENCE_ID_KEY));
            }
            feedbackItems.add(FeedbackItem.builder()
                    .code("NOT_ACCEPTED_BY_RANGE")
                    .level(FeedbackItem.Level.ERROR)
                    .location(validationContext.get(RequestConverter.LOCATION_KEY))
                    .details(details)
                    .build());
        }

        return feedbackItems;
    }
}
