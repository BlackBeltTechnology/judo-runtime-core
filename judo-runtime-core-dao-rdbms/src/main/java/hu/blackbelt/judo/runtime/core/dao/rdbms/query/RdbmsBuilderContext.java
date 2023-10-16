package hu.blackbelt.judo.runtime.core.dao.rdbms.query;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;

import java.util.Map;

@Builder(toBuilder = true)
@Getter
public class RdbmsBuilderContext {
    @NonNull
    public RdbmsBuilder<?> rdbmsBuilder;
    @NonNull
    public EMap<Node, EList<EClass>> ancestors;
    @NonNull
    public EMap<Node, EList<EClass>> descendants;
    public SubSelect parentIdFilterQuery;
    public Map<String, Object> queryParameters;
}
