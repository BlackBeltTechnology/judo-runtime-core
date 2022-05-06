package hu.blackbelt.judo.runtime.core.dao.core.collectors;

import com.google.common.collect.ImmutableMap;
import org.eclipse.emf.ecore.EClass;

import java.util.Collection;
import java.util.Map;

public class EmptyMapIntstanceCollector implements InstanceCollector {
    @Override
    public Map collectGraph(EClass entityType, Collection collection) {
        return ImmutableMap.of();
    }

    @Override
    public InstanceGraph collectGraph(EClass entityType, Object id) {
        return null;
    };

}
