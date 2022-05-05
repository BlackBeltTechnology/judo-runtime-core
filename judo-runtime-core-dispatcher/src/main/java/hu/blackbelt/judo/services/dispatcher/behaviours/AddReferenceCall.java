package hu.blackbelt.judo.services.dispatcher.behaviours;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;

import javax.transaction.TransactionManager;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class AddReferenceCall<ID> extends TransactionalBehaviourCall {

    final DAO dao;
    final AsmUtils asmUtils;
    final IdentifierProvider<ID> identifierProvider;

    public AddReferenceCall(DAO dao, IdentifierProvider<ID> identifierProvider, AsmUtils asmUtils, TransactionManager transactionManager) {
        super(transactionManager);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = asmUtils;
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.ADD_REFERENCE).isPresent();
    }

    @Override
    public Object callInTransaction(Map<String, Object> exchange, EOperation operation) {
        final EReference owner = (EReference) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final String inputParameterName = operation.getEParameters().stream().map(p -> p.getName()).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Input parameter name must be defined"));

        final boolean bound = asmUtils.isBound(operation);
        checkArgument(bound, "Operation must be bound");

        final ID instanceId = (ID) exchange.get(identifierProvider.getName());
        final Collection<ID> referencedIds = ((Collection<Map<String, Object>>) exchange.get(inputParameterName)).stream()
                .map(i -> (ID) i.get(identifierProvider.getName()))
                .collect(Collectors.toList());

        dao.addReferences(owner, instanceId, referencedIds);
        return null;
    }
}
