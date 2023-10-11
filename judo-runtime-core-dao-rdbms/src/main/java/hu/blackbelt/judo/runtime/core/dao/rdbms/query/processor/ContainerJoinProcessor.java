package hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor;

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.ContainerJoin;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsContainerJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsJoin;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Builder
public class ContainerJoinProcessor {
    @NonNull
    private final RdbmsResolver rdbmsResolver;

    public List<RdbmsJoin> process(ContainerJoin join, RdbmsBuilderContext builderContext) {
        final RdbmsBuilder<?> rdbmsBuilder = builderContext.rdbmsBuilder;
        final EMap<Node, EList<EClass>> ancestors = builderContext.ancestors;
        final EMap<Node, EList<EClass>> descendants = builderContext.descendants;
        final SubSelect parentIdFilterQuery = builderContext.parentIdFilterQuery;
        final Map<String, Object> queryParameters = builderContext.queryParameters;

        final EClass targetType = join.getType();
        final Node node = join.getPartner();
        final EList<EReference> references = join.getReferences();
        final EClass sourceType = node != null ? node.getType() : references.get(0).getEReferenceType();

        if (log.isTraceEnabled()) {
            log.trace(" => processing JOIN: {}", join);
            log.trace("    target type: {}", targetType.getName());
            log.trace("    source type: {}", sourceType.getName());
            log.trace("    references: {}", references.stream().map(r -> AsmUtils.getReferenceFQName(r)).collect(Collectors.joining(", ")));
        }

        final List<RdbmsJoin> result = new ArrayList<>();
        int index = 0;
        for (final EReference r : join.getReferences()) {
            result.addAll(rdbmsBuilder.processSimpleJoin(SimpleJoinProcessorParameters.builder()
                    .postfix(RdbmsContainerJoin.POSTFIX + index++)
                    .join(join)
                    .opposite(r)
                    .builderContext(builderContext)
                    .build()));
        }

        result.add(RdbmsContainerJoin.builder()
                .outer(true)
                .tableName(rdbmsResolver.rdbmsTable(targetType).getSqlName())
                .alias(join.getAlias())
                .partnerTable(node)
                .columnName(StatementExecutor.ID_COLUMN_NAME)
                .references(references)
                .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                .build());

        return result;
    }
}
