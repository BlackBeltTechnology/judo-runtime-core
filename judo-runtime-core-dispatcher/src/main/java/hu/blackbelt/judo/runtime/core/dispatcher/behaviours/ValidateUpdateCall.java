package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.mapper.api.Coercer;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EOperation;

import javax.transaction.TransactionManager;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.dao.api.Payload.asPayload;

public class ValidateUpdateCall<ID> extends AlwaysRollbackTransactionalBehaviourCall<ID> {

    final DAO<ID> dao;
    final IdentifierProvider<ID> identifierProvider;
    final AsmUtils asmUtils;
    final Coercer coercer;

    private final MarkedIdRemover<ID> markedIdRemover;

    public ValidateUpdateCall(Context context, DAO<ID> dao, IdentifierProvider<ID> identifierProvider, AsmUtils asmUtils, TransactionManager transactionManager, Coercer coercer) {
        super(context, transactionManager);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = asmUtils;
        markedIdRemover = new MarkedIdRemover<>(identifierProvider.getName());
        this.coercer = coercer;
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.VALIDATE_UPDATE).isPresent();
    }

    @Override
    public Object callInRollbackTransaction(Map<String, Object> exchange, EOperation operation) {
        final EClass owner = (EClass) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final String inputParameterName = operation.getEParameters().stream().map(p -> p.getName()).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Input parameter name must be defined"));

        final boolean bound = AsmUtils.isBound(operation);
        checkArgument(bound, "Operation must be bound");

        @SuppressWarnings("unchecked")
		final Payload payload = asPayload((Map<String, Object>) exchange.get(inputParameterName));
        if (payload.get(identifierProvider.getName()) == null) {
            payload.put(identifierProvider.getName(), exchange.get(identifierProvider.getName()));
        } else {
            payload.put(identifierProvider.getName(),
                    coercer.coerce(exchange.get(identifierProvider.getName()), identifierProvider.getType()));

            @SuppressWarnings("unchecked")
			final ID idInPayload = (ID) payload.get(identifierProvider.getName());
            @SuppressWarnings("unchecked")
			final ID idOfSubject = (ID) exchange.get(identifierProvider.getName());

            if (!Objects.equals(idInPayload, idOfSubject)) {
                throw new IllegalArgumentException("Identifier in payload must match operation subject");
            }
        }

        final Payload result = dao.update(owner, payload, null);
        markedIdRemover.process(result);
        return result;
    }
}
