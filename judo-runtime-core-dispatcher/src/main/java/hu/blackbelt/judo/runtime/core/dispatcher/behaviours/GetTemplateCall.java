package hu.blackbelt.judo.runtime.core.dispatcher.behaviours;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EOperation;

import java.util.Map;

public class GetTemplateCall<ID> implements BehaviourCall<ID> {

    final DAO<ID> dao;
    final AsmUtils asmUtils;

    public GetTemplateCall(DAO<ID> dao, AsmUtils asmUtils) {
        this.dao = dao;
        this.asmUtils = asmUtils;
    }

    @Override
    public boolean isSuitableForOperation(EOperation operation) {
        return AsmUtils.getBehaviour(operation).filter(o -> o == AsmUtils.OperationBehaviour.GET_TEMPLATE).isPresent();
    }

    @Override
    public Object call(final Map<String, Object> exchange, final EOperation operation) {
        final EClass owner = (EClass) asmUtils.getOwnerOfOperationWithDefaultBehaviour(operation)
                .orElseThrow(() -> new IllegalArgumentException("Invalid model"));

        return dao.getDefaultsOf(owner);
    }
}
