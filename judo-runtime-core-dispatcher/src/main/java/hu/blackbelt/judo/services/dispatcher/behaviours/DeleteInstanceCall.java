package hu.blackbelt.judo.services.dispatcher.behaviours;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EOperation;

import javax.transaction.TransactionManager;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class DeleteInstanceCall<ID> extends TransactionalBehaviourCall {

    final DAO dao;
    final IdentifierProvider<ID> identifierProvider;
    final AsmUtils asmUtils;

    public DeleteInstanceCall(DAO dao, IdentifierProvider<ID> identifierProvider, AsmUtils asmUtils, TransactionManager transactionManager) {
        super(transactionManager);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = asmUtils;
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.DELETE_INSTANCE).isPresent();
    }

    @Override
    public Object callInTransaction(Map<String, Object> exchange, EOperation operation) {
        final EClass owner = (EClass) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final boolean bound = asmUtils.isBound(operation);
        checkArgument(bound, "Operation must be bound");

        dao.delete(owner, exchange.get(identifierProvider.getName()));

        return null;
    }
}
