package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.Attribute;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsColumn;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.common.util.UniqueEList;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;

import java.util.Map;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public class AttributeMapper extends RdbmsMapper<Attribute> {

    @NonNull
    private final RdbmsBuilder rdbmsBuilder;

    @Override
    public Stream<RdbmsColumn> map(final Attribute attribute, final EMap<Node, EList<EClass>> ancestors, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
        final EClass sourceType = attribute.getNode().getType();
        final EAttribute sourceAttribute = attribute.getSourceAttribute();
        final EClass attributeContainer = sourceAttribute.getEContainingClass();

        log.trace("Checking attributes");
        log.trace(" - source type: {}", sourceType.getName());
        log.trace(" - source attribute: {}", sourceAttribute.getName());
        log.trace(" - source attribute container: {}", attributeContainer.getName());

        // add column to list
        final String postfix;
        if (!AsmUtils.equals(sourceType, attributeContainer)) {  // inherited attribute
            log.trace("   - found inherited attribute: {}", sourceAttribute.getName());

            if (!ancestors.containsKey(attribute.getNode())) {
                ancestors.put(attribute.getNode(), new UniqueEList<>());
            }
            // add ancestor for a given attribute
            ancestors.get(attribute.getNode()).add(attributeContainer);
            postfix = rdbmsBuilder.getAncestorPostfix(attributeContainer);
        } else {
            postfix = "";
        }

        return getTargets(attribute).map(t -> RdbmsColumn.builder()
                .partnerTable(attribute.getNode())
                .partnerTablePostfix(postfix)
                .columnName(rdbmsBuilder.getColumnName(attribute.getSourceAttribute()))
                .target(t.getTarget())
                .targetAttribute(t.getTargetAttribute())
                .alias(t.getAlias())
                .sourceDomainConstraints(RdbmsField.getDomainConstraints(attribute.getSourceAttribute()))
                .build());
    }
}
