package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import org.eclipse.emf.ecore.EOperation;
import org.eclipse.emf.ecore.EReference;

import javax.transaction.TransactionManager;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.dao.api.Payload.asPayload;

public class CreateInstanceCall<ID> extends TransactionalBehaviourCall {

    final DAO dao;
    final AsmUtils asmUtils;
    final IdentifierProvider<ID> identifierProvider;

    public CreateInstanceCall(DAO dao, IdentifierProvider<ID> identifierProvider, AsmUtils asmUtils, TransactionManager transactionManager) {
        super(transactionManager);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = asmUtils;
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.CREATE_INSTANCE).isPresent();
    }

    @Override
    public Object callInTransaction(Map<String, Object> exchange, EOperation operation) {
        final EReference owner = (EReference) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final String inputParameterName = operation.getEParameters().stream().map(p -> p.getName()).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Input parameter name must be defined"));

        final boolean bound = asmUtils.isBound(operation);
        
        if (AsmUtils.annotatedAsTrue(owner, "access") || !asmUtils.isMappedTransferObjectType(owner.getEContainingClass())) {
            checkArgument(!bound, "Operation must be unbound");
            return dao.create(owner.getEReferenceType(),
                    asPayload((Map<String, Object>) exchange.get(inputParameterName)), null);
        } else {
            checkArgument(bound, "Operation must be bound");
            return dao.createNavigationInstanceAt(exchange.get(identifierProvider.getName()),
                    owner,
                    asPayload((Map<String, Object>) exchange.get(inputParameterName)), null);
        }
    }
}
