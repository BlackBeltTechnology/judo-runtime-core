package hu.blackbelt.judo.services.dao.core.collectors;

import org.eclipse.emf.ecore.EClass;

import java.util.Collection;
import java.util.Map;

public interface InstanceCollector<ID> {

    Map<ID, InstanceGraph<ID>> collectGraph(EClass entityType, Collection<ID> ids);

    InstanceGraph<ID> collectGraph(EClass entityType, ID ds);

}
