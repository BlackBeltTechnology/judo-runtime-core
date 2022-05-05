package hu.blackbelt.judo.services.dao.rdbms.query.mappers;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.IdAttribute;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.services.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.services.dao.rdbms.query.model.RdbmsColumn;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.common.util.UniqueEList;
import org.eclipse.emf.ecore.EClass;

import java.util.Map;
import java.util.stream.Stream;

@Slf4j
public class IdAttributeMapper extends RdbmsMapper<IdAttribute> {

    @Override
    public Stream<RdbmsColumn> map(final IdAttribute idAttribute, final EMap<Node, EList<EClass>> ancestors, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
        final EClass sourceType = idAttribute.getNode().getType();
        sourceType.getEAllSuperTypes().forEach(superType -> {
            log.trace("   - found super type: {}", AsmUtils.getClassifierFQName(superType));
            if (!ancestors.containsKey(idAttribute.getNode())) {
                ancestors.put(idAttribute.getNode(), new UniqueEList<>());
            }
            // add ancestor for a given attribute
            ancestors.get(idAttribute.getNode()).add(superType);
        });

        return getTargets(idAttribute)
                .map(t -> RdbmsColumn.builder()
                        .partnerTable(idAttribute.getNode())
                        .columnName(StatementExecutor.ID_COLUMN_NAME)
                        .alias(idAttribute.getNode().getAlias() + "_" + StatementExecutor.ID_COLUMN_NAME)
                        .build());
    }
}
