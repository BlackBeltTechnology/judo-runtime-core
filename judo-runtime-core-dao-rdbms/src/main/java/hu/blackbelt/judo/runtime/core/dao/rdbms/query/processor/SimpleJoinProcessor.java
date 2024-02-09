package hu.blackbelt.judo.runtime.core.dao.rdbms.query.processor;

/*-
 * #%L
 * JUDO Runtime Core :: Parent
 * %%
 * Copyright (C) 2018 - 2022 BlackBelt Technology
 * %%
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the Eclipse
 * Public License, v. 2.0 are satisfied: GNU General Public License, version 2
 * with the GNU Classpath Exception which is
 * available at https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 * #L%
 */

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.Join;
import hu.blackbelt.judo.meta.query.Node;
import hu.blackbelt.judo.meta.query.SubSelectFeature;
import hu.blackbelt.judo.meta.rdbmsRules.Rule;
import hu.blackbelt.judo.meta.rdbmsRules.Rules;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.AncestorNameFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.DescendantNameFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilderContext;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsFunction;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsNavigationFilter;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsJoin;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.RdbmsTableJoin;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.UniqueEList;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Builder
@Slf4j
public class SimpleJoinProcessor<ID> {

    @NonNull
    private final RdbmsResolver rdbmsResolver;

    @NonNull
    private final Rules rules;

    @NonNull
    private final AncestorNameFactory ancestorNameFactory;

    @NonNull
    private final DescendantNameFactory descendantNameFactory;

    public List<RdbmsJoin> process(SimpleJoinProcessorParameters params) {
        final String postfix = params.getPostfix();
        final Join join = params.getJoin();
        final EReference reference = params.getReference();
        final EReference opposite = params.getOpposite();
        final RdbmsBuilderContext builderContext = params.getBuilderContext();
        final Map<Node, List<EClass>> ancestors = builderContext.getAncestors();


        final EClass targetType = join.getType();
        final Node node = join.getPartner();
        final EClass sourceType = node.getType();


        if (log.isTraceEnabled()) {
            log.trace(params.toString());
            log.trace(" => processing JOIN: {}", join);
            log.trace("    target type: {}", targetType.getName());
            log.trace("    source type: {}", sourceType.getName());
        }

        final String tableName = rdbmsResolver.rdbmsTable(targetType).getSqlName();

        // create builder for RDBMS JOIN definition
        @SuppressWarnings("rawtypes")
        final RdbmsJoin.RdbmsJoinBuilder builder = RdbmsTableJoin.builder()
                .outer(true)
                .tableName(tableName)
                .alias(join.getAlias() + postfix)
                .partnerTable(node);

        final Rule rule;
        EClass sourceContainer = null;
        if (reference != null) {
            // get RDBMS rule of a given reference
            rule = rules.getRuleFromReference(reference);

            sourceContainer = reference.getEContainingClass();
            if (log.isTraceEnabled()) {
                log.trace("    reference: {}", reference.getName());
                log.trace("    reference container: {}", sourceContainer.getName());
            }
        } else {
            rule = null;
        }

        final Rule oppositeRule;
        if (opposite != null) {
            oppositeRule = rules.getRuleFromReference(opposite);

            final EClass oppositeContainer = opposite.getEReferenceType();
            if (log.isTraceEnabled()) {
                log.trace("    opposite: {}", opposite.getName());
                log.trace("    opposite reference container: {}", oppositeContainer.getName());
            }

            if (sourceContainer == null) {
                sourceContainer = oppositeContainer;
            }
        } else {
            oppositeRule = null;
        }

        if (!AsmUtils.equals(sourceType, sourceContainer)) { // reference is inherited from another class, resolve ancestor too
            if (log.isTraceEnabled()) {
                log.trace("  - reference '{}' is inherited", reference.getName());
            }

            if (!ancestors.containsKey(node)) {
                ancestors.put(node, new UniqueEList<>());
            }
            ancestors.get(node).add(sourceContainer);
            builder.partnerTablePostfix(ancestorNameFactory.getAncestorPostfix(sourceContainer));
        }
        if (!targetType.getEAllSuperTypes().isEmpty()) {
            if (!ancestors.containsKey(join)) {
                ancestors.put(join, new UniqueEList<>());
            }
            ancestors.get(join).addAll(targetType.getEAllSuperTypes());
        }

        if (rule != null && rule.isForeignKey()) { // reference is owned by source class, target class has reference to the ID with different name
            if (log.isTraceEnabled()) {
                log.trace("  - reference '{}' is foreign key", reference.getName());
            }
            builder.columnName(StatementExecutor.ID_COLUMN_NAME).partnerColumnName(rdbmsResolver.rdbmsField(reference).getSqlName());
        } else if (rule != null && rule.isInverseForeignKey()) {  // reference is owned by target class, source class has reference to the ID with different name
            if (log.isTraceEnabled()) {
                log.trace("  - reference '{}' is inverse foreign key", reference.getName());
            }
            builder.columnName(rdbmsResolver.rdbmsField(reference).getSqlName()).partnerColumnName(StatementExecutor.ID_COLUMN_NAME);
        } else if (rule != null && rule.isJoinTable()) { // JOIN tables are not supported yet
            if (log.isTraceEnabled()) {
                log.trace("  - reference '{}' is JOIN table", reference.getName());
            }
            builder.columnName(StatementExecutor.ID_COLUMN_NAME).partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                    .junctionTableName(rdbmsResolver.rdbmsJunctionTable(reference).getSqlName())
                    .junctionColumnName(rdbmsResolver.rdbmsJunctionField(reference).getSqlName())
                    .junctionOppositeColumnName(rdbmsResolver.rdbmsJunctionOppositeField(reference).getSqlName());
        } else if (oppositeRule != null && oppositeRule.isForeignKey()) { // reference is owned by source class, target class has reference to the ID with different name (defined by opposite reference)
            if (log.isTraceEnabled()) {
                log.trace("  - opposite reference '{}' is foreign key", opposite.getName());
            }

            builder.columnName(rdbmsResolver.rdbmsField(opposite).getSqlName()).partnerColumnName(StatementExecutor.ID_COLUMN_NAME);
        } else if (oppositeRule != null && oppositeRule.isInverseForeignKey()) {  // reference is owned by target class, source class has reference to the ID with different name (defined by opposite reference)
            if (log.isTraceEnabled()) {
                log.trace("  - opposite reference '{}' is inverse foreign key", opposite.getName());
            }
            builder.columnName(StatementExecutor.ID_COLUMN_NAME).partnerColumnName(rdbmsResolver.rdbmsField(opposite).getSqlName());
        } else if (oppositeRule != null && oppositeRule.isJoinTable()) { // JOIN tables are not supported yet
            if (log.isTraceEnabled()) {
                log.trace("  - opposite reference '{}' is JOIN table", opposite.getName());
            }
            builder.columnName(StatementExecutor.ID_COLUMN_NAME).partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                    .junctionTableName(rdbmsResolver.rdbmsJunctionTable(opposite).getSqlName())
                    .junctionColumnName(rdbmsResolver.rdbmsJunctionField(opposite).getSqlName())
                    .junctionOppositeColumnName(rdbmsResolver.rdbmsJunctionOppositeField(opposite).getSqlName());
        } else {
            throw new IllegalStateException("Invalid reference");
        }

        if (!join.getFilters().isEmpty() && join.getFilters().stream().noneMatch(filter -> filter.getFeatures().stream().anyMatch(feature -> feature instanceof SubSelectFeature))) {
            builder.onConditions(join.getFilters().stream()
                    .map(f -> RdbmsFunction.builder()
                            .pattern("EXISTS ({0})")
                            .parameter(
                                    RdbmsNavigationFilter.<ID>builder()
                                            .builderContext(builderContext)
                                            .filter(f)
                                            .build())
                            .build())
                    .collect(Collectors.toList()));
        }

        final RdbmsJoin rdbmsJoin = builder.build();

        final List<RdbmsJoin> joins = new ArrayList<>();
        joins.add(rdbmsJoin);

        if (ancestors.containsKey(join)) {
            builderContext.getRdbmsBuilder().addAncestorJoins(joins, join, builderContext);
        }

        return joins;
    }
}
