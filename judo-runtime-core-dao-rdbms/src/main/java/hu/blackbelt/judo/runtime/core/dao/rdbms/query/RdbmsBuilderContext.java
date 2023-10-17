package hu.blackbelt.judo.runtime.core.dao.rdbms.query;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;

import java.util.HashMap;
import java.util.Map;

@Builder(toBuilder = true)
@Getter
@ToString
public class RdbmsBuilderContext {
    @NonNull
    private RdbmsBuilder<?> rdbmsBuilder;

    @Builder.Default
    private EMap<Node, EList<EClass>> ancestors = ECollections.asEMap(new HashMap<>());;
    @Builder.Default
    private EMap<Node, EList<EClass>> descendants = ECollections.asEMap(new HashMap<>());;
    private SubSelect parentIdFilterQuery;
    private Map<String, Object> queryParameters;

}
