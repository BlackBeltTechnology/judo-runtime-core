package hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor;

import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.AncestorNameFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.DescendantNameFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsTableJoin;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.ecore.EClass;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Builder
@Slf4j
public class AncestorJoinsProcessor {

    @NonNull
    private final RdbmsResolver rdbmsResolver;

    @NonNull
    private final AncestorNameFactory ancestorNameFactory;

    @NonNull
    private final DescendantNameFactory descendantNameFactory;

    public void process(final Collection<RdbmsJoin> joins, final Node node, RdbmsBuilderContext builderContext) {
        final EMap<Node, EList<EClass>> ancestors = builderContext.getAncestors();
        if (log.isTraceEnabled()) {
            log.trace("Node:       " + node);
            log.trace("Joins:      " + joins);
            log.trace("Ancestors:  " + ancestors);
        }

        final EList<EClass> list;
        if (ancestors.containsKey(node)) {
            list = ancestors.get(node);
        } else if (node.eContainer() instanceof SubSelect && ancestors.containsKey(((SubSelect) node.eContainer()).getSelect())) {
            list = ancestors.get(((SubSelect) node.eContainer()).getSelect());
        } else if (node.eContainer() instanceof Node && ancestors.containsKey(node.eContainer())) {
            list = ancestors.get(node.eContainer());
        } else {
            list = ECollections.emptyEList();
        }

        List<RdbmsJoin> newJoins = list.stream()
                .filter(c -> joins.stream()
                        .noneMatch(j -> Objects.equals(node.getAlias() + ancestorNameFactory.getAncestorPostfix(c), j.getAlias())))
                .map(ancestor -> RdbmsTableJoin.builder()
                        .tableName(rdbmsResolver.rdbmsTable(ancestor).getSqlName())
                        .alias(node.getAlias() + ancestorNameFactory.getAncestorPostfix(ancestor))
                        .columnName(StatementExecutor.ID_COLUMN_NAME)
                        .partnerTable(node)
                        .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                        .outer(true)
                        .build())
                .collect(Collectors.toList());

        joins.addAll(newJoins);
    }


    /*
    public void addDescendantJoins(final Collection<RdbmsJoin> joins, final Node node, final EMap<Node, EList<EClass>> descendants) {
        final EList<EClass> list;
        if (descendants.containsKey(node)) {
            list = descendants.get(node);
        } else if (node.eContainer() instanceof SubSelect && descendants.containsKey(((SubSelect) node.eContainer()).getSelect())) {
            list = descendants.get(((SubSelect) node.eContainer()).getSelect());
        } else if (node.eContainer() instanceof Node && descendants.containsKey(node.eContainer())) {
            list = descendants.get(node.eContainer());
        } else {
            list = ECollections.emptyEList();
        }

        List<RdbmsJoin> newJoins = list.stream()
                .filter(c -> joins.stream().noneMatch(j -> Objects.equals(node.getAlias() + descendantNameFactory.getDescendantPostfix(c), j.getAlias())))
                .map(descendant -> RdbmsTableJoin.builder()
                        .tableName(rdbmsResolver.rdbmsTable(descendant).getSqlName())
                        .alias(node.getAlias() + descendantNameFactory.getDescendantPostfix(descendant))
                        .columnName(StatementExecutor.ID_COLUMN_NAME)
                        .partnerTable(node)
                        .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                        .outer(true)
                        .build())
                .collect(Collectors.toList());

        joins.addAll(newJoins);
    }

    public Collection<RdbmsJoin> getDescendantJoins(final Node node, final EMap<Node, EList<EClass>> descendants, final Collection<RdbmsJoin> joins) {
        final EList<EClass> list;
        if (descendants.containsKey(node)) {
            list = descendants.get(node);
        } else if (node.eContainer() instanceof SubSelect && descendants.containsKey(((SubSelect) node.eContainer()).getSelect())) {
            list = descendants.get(((SubSelect) node.eContainer()).getSelect());
        } else if (node.eContainer() instanceof Node && descendants.containsKey(node.eContainer())) {
            list = descendants.get(node.eContainer());
        } else {
            list = ECollections.emptyEList();
        }

        return list.stream()
                .filter(c -> joins.stream().noneMatch(j -> Objects.equals(node.getAlias() + descendantNameFactory.getDescendantPostfix(c), j.getAlias())))
                .map(descendant -> RdbmsTableJoin.builder()
                        .tableName(rdbmsResolver.rdbmsTable(descendant).getSqlName())
                        .alias(node.getAlias() + descendantNameFactory.getDescendantPostfix(descendant))
                        .columnName(StatementExecutor.ID_COLUMN_NAME)
                        .partnerTable(node)
                        .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                        .outer(true)
                        .build())
                .collect(Collectors.toList());
    }

     */

}