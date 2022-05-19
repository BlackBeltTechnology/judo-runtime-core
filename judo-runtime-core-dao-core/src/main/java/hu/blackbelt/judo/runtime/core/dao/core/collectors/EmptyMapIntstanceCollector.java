package hu.blackbelt.judo.runtime.core.dao.core.collectors;

import com.google.common.collect.ImmutableMap;
import org.eclipse.emf.ecore.EClass;

import java.util.Collection;
import java.util.Map;

public class EmptyMapIntstanceCollector<ID> implements InstanceCollector<ID> {
    @Override
    public Map<ID, InstanceGraph<ID>> collectGraph(EClass entityType, Collection<ID> collection) {
        return ImmutableMap.of();
    }

    @Override
    public InstanceGraph<ID> collectGraph(EClass entityType, ID id) {
        return null;
    };

}
