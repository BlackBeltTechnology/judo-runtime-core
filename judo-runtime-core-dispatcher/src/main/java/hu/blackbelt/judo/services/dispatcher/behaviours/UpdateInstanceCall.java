package hu.blackbelt.judo.services.dispatcher.behaviours;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.mapper.api.Coercer;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EOperation;

import javax.transaction.TransactionManager;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.dao.api.Payload.asPayload;

public class UpdateInstanceCall<ID> extends TransactionalBehaviourCall {

    final DAO dao;
    final IdentifierProvider<ID> identifierProvider;
    final AsmUtils asmUtils;
    final Coercer coercer;

    public UpdateInstanceCall(DAO dao, IdentifierProvider<ID> identifierProvider, AsmUtils asmUtils, TransactionManager transactionManager, Coercer coercer) {
        super(transactionManager);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = asmUtils;
        this.coercer = coercer;
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.UPDATE_INSTANCE).isPresent();
    }

    @Override
    public Object callInTransaction(Map<String, Object> exchange, EOperation operation) {
        final EClass owner = (EClass) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final String inputParameterName = operation.getEParameters().stream().map(p -> p.getName()).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Input parameter name must be defined"));

        final boolean bound = asmUtils.isBound(operation);
        checkArgument(bound, "Operation must be bound");

        final Payload payload = asPayload((Map<String, Object>) exchange.get(inputParameterName));
        if (payload.get(identifierProvider.getName()) == null) {
            payload.put(identifierProvider.getName(), exchange.get(identifierProvider.getName()));
        } else {
            payload.put(identifierProvider.getName(),
                    coercer.coerce(exchange.get(identifierProvider.getName()), identifierProvider.getType()));

            final ID idInPayload = (ID) payload.get(identifierProvider.getName());
            final ID idOfSubject = (ID) exchange.get(identifierProvider.getName());

            if (!Objects.equals(idInPayload, idOfSubject)) {
                throw new IllegalArgumentException("Identifier in payload must match operation subject");
            }
        }

        return dao.update(owner, payload, null);
    }
}
