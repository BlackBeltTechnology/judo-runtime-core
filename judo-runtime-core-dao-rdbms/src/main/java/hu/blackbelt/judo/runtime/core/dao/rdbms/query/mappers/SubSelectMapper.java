package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

import hu.blackbelt.judo.meta.query.Filter;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.OrderBy;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsResultSet;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class SubSelectMapper extends RdbmsMapper<SubSelect> {

    @NonNull
    private final RdbmsBuilder rdbmsBuilder;

    @Override
    public Stream<RdbmsResultSet> map(final SubSelect subSelect, final EMap<Node, EList<EClass>> ancestors, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
        final Object container = subSelect.eContainer();
        final boolean withoutFeatures = container instanceof Filter || container instanceof OrderBy || subSelect.getSelect().isAggregated();
        return Collections.singleton(
                RdbmsResultSet.builder()
                        .query(subSelect)
                        .filterByInstances(false)
                        .parentIdFilterQuery(parentIdFilterQuery)
                        .rdbmsBuilder(rdbmsBuilder)
                        .seek(null)
                        .withoutFeatures(withoutFeatures)
                        .mask(null)
                        .queryParameters(queryParameters)
                        .skipParents(false)
                        .build())
                .stream();
    }
}
