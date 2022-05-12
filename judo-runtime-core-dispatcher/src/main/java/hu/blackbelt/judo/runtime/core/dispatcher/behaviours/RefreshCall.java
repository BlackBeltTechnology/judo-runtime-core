package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dispatcher.api.Context;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.mapper.api.Coercer;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EOperation;

import javax.transaction.TransactionManager;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class RefreshCall<ID> extends AlwaysRollbackTransactionalBehaviourCall<ID> {

    final DAO<ID> dao;
    final IdentifierProvider<ID> identifierProvider;
    final AsmUtils asmUtils;
    private final QueryCustomizerParameterProcessor<ID> queryCustomizerParameterProcessor;

    public RefreshCall(Context context, DAO<ID> dao, IdentifierProvider<ID> identifierProvider, AsmUtils asmUtils, TransactionManager transactionManager, Coercer coercer, boolean caseInsensitiveLike) {
        super(context, transactionManager);
        this.dao = dao;
        this.identifierProvider = identifierProvider;
        this.asmUtils = asmUtils;
        queryCustomizerParameterProcessor = new QueryCustomizerParameterProcessor<ID>(asmUtils, caseInsensitiveLike, identifierProvider, coercer);
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.REFRESH).isPresent();
    }

    @SuppressWarnings("unchecked")
	@Override
    public Object callInRollbackTransaction(Map<String, Object> exchange, EOperation operation) {
        final EClass owner = (EClass) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        final boolean bound = AsmUtils.isBound(operation);
        checkArgument(bound, "Operation must be bound");

		final Optional<Map<String, Object>> queryCustomizerParameter = operation.getEParameters().stream().map(p -> p.getName())
                .findFirst()
                .map(inputParameter -> (Map<String, Object>) exchange.get(inputParameter));
        @SuppressWarnings("rawtypes")
		final DAO.QueryCustomizer queryCustomizer = queryCustomizerParameterProcessor.build(queryCustomizerParameter.orElse(null), owner);

        return dao.searchByIdentifier(owner, (ID) exchange.get(identifierProvider.getName()), queryCustomizer).get();
    }
}
