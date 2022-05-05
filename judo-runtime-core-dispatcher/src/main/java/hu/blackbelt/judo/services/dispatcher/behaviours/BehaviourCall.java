package hu.blackbelt.judo.services.dispatcher.behaviours;

import org.eclipse.emf.ecore.EOperation;

import java.util.Map;

public interface BehaviourCall {
    boolean isSuitableForOperation(EOperation operation);

    Object call(final Map<String, Object> exchange,
                final EOperation operation);
}
