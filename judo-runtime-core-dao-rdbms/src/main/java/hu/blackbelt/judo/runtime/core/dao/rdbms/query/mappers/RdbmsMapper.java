package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import lombok.Builder;
import lombok.Getter;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

public abstract class RdbmsMapper<T extends ParameterType> {

    private static final int MAX_ALIAS_LENGTH_WITHOUT_INDEX = 25;

    public abstract Stream<? extends RdbmsField> map(T parameter, EMap<Node, EList<EClass>> ancestors, SubSelect parentIdFilterQuery, Map<String, Object> queryParameters);

    public static Stream<RdbmsTarget> getTargets(final Feature feature) {
        if (feature.getTargetMappings().isEmpty()) {
            return Collections.singleton(RdbmsTarget.builder()
                    .alias(getAttributeOrFeatureName(null, feature))
                    .build()).stream();
        } else {
            return feature.getTargetMappings().stream()
                    .map(tm -> RdbmsTarget.builder()
                            .target(tm.getTarget())
                            .targetAttribute(tm.getTargetAttribute())
                            .alias(getAttributeOrFeatureName(tm.getTargetAttribute(), feature))
                            .build());
        }
    }

    public static String getAttributeOrFeatureName(final EAttribute attribute, final Feature feature) {
        if (attribute != null) {
            return attribute.getName().length() > MAX_ALIAS_LENGTH_WITHOUT_INDEX ? attribute.getName().substring(0, MAX_ALIAS_LENGTH_WITHOUT_INDEX - 10) + attribute.hashCode() : attribute.getName();
        } else if (feature != null) {
            return "__f" + ((Node) feature.eContainer()).getFeatures().indexOf(feature);
        } else {
            throw new IllegalArgumentException("Attribute or feature is mandatory");
        }
    }

    @Getter
    @Builder
    public static class RdbmsTarget {
        private final Target target;
        private final String alias;
        private final EAttribute targetAttribute;
    }
}
