package hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers;

import hu.blackbelt.judo.meta.query.EntityTypeName;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsEntityTypeName;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsField;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class EntityTypeNameMapper extends RdbmsMapper<EntityTypeName> {

    @NonNull
    private final RdbmsBuilder rdbmsBuilder;

    @Override
    public Stream<? extends RdbmsField> map(final EntityTypeName entityTypeName, final EMap<Node, EList<EClass>> ancestors, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
        return Collections.singleton(RdbmsEntityTypeName.builder()
                .tableName(rdbmsBuilder.getTableName(entityTypeName.getType()))
                .type(entityTypeName.getType())
                .build()).stream();
    }
}
