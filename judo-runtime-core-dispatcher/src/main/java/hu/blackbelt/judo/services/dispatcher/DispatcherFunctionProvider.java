package hu.blackbelt.judo.services.dispatcher;

import hu.blackbelt.judo.dao.api.Payload;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EOperation;

import java.util.function.Function;

public interface DispatcherFunctionProvider {

    EMap<EOperation, Function<Payload, Payload>> getSdkFunctions();

    EMap<EOperation, Function<Payload, Payload>> getScriptFunctions();

    default EList<EOperation> getMissingScripts() {
        return ECollections.emptyEList();
    }

    default EList<EOperation> getMissingSdkOperations() {
        return ECollections.emptyEList();
    }
}
