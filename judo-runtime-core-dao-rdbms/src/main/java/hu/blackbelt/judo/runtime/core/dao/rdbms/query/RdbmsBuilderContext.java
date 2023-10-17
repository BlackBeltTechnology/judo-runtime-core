package hu.blackbelt.judo.runtime.core.dao.rdbms.query;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;

import java.util.Map;

@Builder(toBuilder = true)
@Getter
@ToString
public class RdbmsBuilderContext {
    private int level = 1;

    @NonNull
    private RdbmsBuilder<?> rdbmsBuilder;
    @NonNull
    private EMap<Node, EList<EClass>> ancestors;
    @NonNull
    private EMap<Node, EList<EClass>> descendants;
    private SubSelect parentIdFilterQuery;
    private Map<String, Object> queryParameters;

}
