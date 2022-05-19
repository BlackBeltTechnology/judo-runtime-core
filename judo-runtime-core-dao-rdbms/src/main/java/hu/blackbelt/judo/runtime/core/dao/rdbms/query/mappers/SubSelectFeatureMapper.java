package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsColumn;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;

import java.util.Map;
import java.util.stream.Stream;

public class SubSelectFeatureMapper extends RdbmsMapper<SubSelectFeature> {

    @Override
    public Stream<RdbmsField> map(final SubSelectFeature feature, final EMap<Node, EList<EClass>> ancestors, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
        final String pattern;
        if ((feature.getFeature() instanceof Function) && ((Function) feature.getFeature()).getSignature() == FunctionSignature.COUNT) {
            pattern = "COALESCE({0}.{1}, 0)";
        } else {
            pattern = null;
        }

        return getTargets(feature).map(t -> RdbmsColumn.builder()
                .target(t.getTarget())
                .targetAttribute(t.getTargetAttribute())
                .partnerTable(feature.getSubSelect())
                .columnName(getTargets(feature.getFeature()).map(tt -> tt.getAlias() + (tt.getTarget() != null ? "_" + tt.getTarget().getIndex() : "")).findAny().get())
                .pattern(pattern)
                .alias(t.getAlias())
                .build());
    }
}
