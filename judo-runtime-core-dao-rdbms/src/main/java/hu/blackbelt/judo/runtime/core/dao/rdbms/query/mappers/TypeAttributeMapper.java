package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.meta.query.TypeAttribute;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsColumn;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.common.util.UniqueEList;
import org.eclipse.emf.ecore.EClass;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
public class TypeAttributeMapper extends RdbmsMapper<TypeAttribute> {

    @Override
    public Stream<RdbmsColumn> map(final TypeAttribute typeAttribute, final EMap<Node, EList<EClass>> ancestors, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
        final EClass sourceType = typeAttribute.getNode().getType();
        sourceType.getEAllSuperTypes().forEach(superType -> {
            log.trace("   - found super type: {}", AsmUtils.getClassifierFQName(superType));
            if (!ancestors.containsKey(typeAttribute.getNode())) {
                ancestors.put(typeAttribute.getNode(), new UniqueEList<>());
            }
            // add ancestor for a given attribute
            ancestors.get(typeAttribute.getNode()).add(superType);
        });

        return getTargets(typeAttribute)
                .flatMap(t -> Arrays.asList(
                        RdbmsColumn.builder()
                                .partnerTable(typeAttribute.getNode())
                                .columnName(StatementExecutor.ENTITY_TYPE_COLUMN_NAME)
                                .alias(typeAttribute.getNode().getAlias() + "_" + StatementExecutor.ENTITY_TYPE_COLUMN_NAME)
                                .build(),
                        RdbmsColumn.builder()
                                .partnerTable(typeAttribute.getNode())
                                .columnName(StatementExecutor.ENTITY_VERSION_COLUMN_NAME)
                                .alias(typeAttribute.getNode().getAlias() + "_" + StatementExecutor.ENTITY_VERSION_COLUMN_NAME)
                                .build(),
                        RdbmsColumn.builder()
                                .partnerTable(typeAttribute.getNode())
                                .columnName(StatementExecutor.ENTITY_CREATE_USERNAME_COLUMN_NAME)
                                .alias(typeAttribute.getNode().getAlias() + "_" + StatementExecutor.ENTITY_CREATE_USERNAME_COLUMN_NAME)
                                .build(),
                        RdbmsColumn.builder()
                                .partnerTable(typeAttribute.getNode())
                                .columnName(StatementExecutor.ENTITY_CREATE_USER_ID_COLUMN_NAME)
                                .alias(typeAttribute.getNode().getAlias() + "_" + StatementExecutor.ENTITY_CREATE_USER_ID_COLUMN_NAME)
                                .build(),
                        RdbmsColumn.builder()
                                .partnerTable(typeAttribute.getNode())
                                .columnName(StatementExecutor.ENTITY_CREATE_TIMESTAMP_COLUMN_NAME)
                                .alias(typeAttribute.getNode().getAlias() + "_" + StatementExecutor.ENTITY_CREATE_TIMESTAMP_COLUMN_NAME)
                                .build(),
                        RdbmsColumn.builder()
                                .partnerTable(typeAttribute.getNode())
                                .columnName(StatementExecutor.ENTITY_UPDATE_USERNAME_COLUMN_NAME)
                                .alias(typeAttribute.getNode().getAlias() + "_" + StatementExecutor.ENTITY_UPDATE_USERNAME_COLUMN_NAME)
                                .build(),
                        RdbmsColumn.builder()
                                .partnerTable(typeAttribute.getNode())
                                .columnName(StatementExecutor.ENTITY_UPDATE_USER_ID_COLUMN_NAME)
                                .alias(typeAttribute.getNode().getAlias() + "_" + StatementExecutor.ENTITY_UPDATE_USER_ID_COLUMN_NAME)
                                .build(),
                        RdbmsColumn.builder()
                                .partnerTable(typeAttribute.getNode())
                                .columnName(StatementExecutor.ENTITY_UPDATE_TIMESTAMP_COLUMN_NAME)
                                .alias(typeAttribute.getNode().getAlias() + "_" + StatementExecutor.ENTITY_UPDATE_TIMESTAMP_COLUMN_NAME)
                                .build()
                ).stream());
    }
}
