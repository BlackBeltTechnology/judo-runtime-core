package hu.blackbelt.judo.runtime.core.dao.rdbms.executors;

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

import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.core.statements.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.*;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.getClassifierFQName;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.jooq.lambda.Unchecked.consumer;

/**
 * Executing Delete statements. It have use {@link RemoveReferenceStatement} instances too because some of them
 * are embedded in the delete statements directly.
 *
 * @param <ID>
 */
@Slf4j(topic = "dao-rdbms")
class DeleteStatementExecutor<ID> extends StatementExecutor<ID> {

    private final RdbmsReferenceUtil<ID> rdbmsReferenceUtil;

    @Builder
    public DeleteStatementExecutor(
            @NonNull AsmModel asmModel,
            @NonNull RdbmsModel rdbmsModel,
            @NonNull TransformationTraceService transformationTraceService,
            @NonNull RdbmsParameterMapper<ID> rdbmsParameterMapper,
            @NonNull RdbmsResolver rdbmsResolver,
            @NonNull Coercer coercer,
            @NonNull IdentifierProvider<ID> identifierProvider) {
        super(asmModel, rdbmsModel, transformationTraceService, rdbmsParameterMapper, rdbmsResolver, coercer, identifierProvider);
        rdbmsReferenceUtil = new RdbmsReferenceUtil<>(asmModel, rdbmsModel, transformationTraceService);
    }

    /**
     * It executes {@link InsertStatement} instances with dependency order, uses
     * topological order between foreign key dependencies.
     *
     * It makes it in two phase, the first deletes the record without any attributes and mandatory references, in
     * second phase deletes the rest.
     *
     * @param jdbcTemplate
     * @param deleteStatements
     * @param removeReferenceStatements
     * @throws SQLException
     */
    public void executeDeleteStatements(NamedParameterJdbcTemplate jdbcTemplate,
                                        Collection<DeleteStatement<ID>> deleteStatements,
                                        Collection<RemoveReferenceStatement<ID>> removeReferenceStatements) {

        // Collect all information required to build dependencies between nodes.
        Set<RdbmsReference<ID>> deleteRdbmsReferences = toRdbmsReferences(deleteStatements, removeReferenceStatements);

        AsmUtils asmUtils = new AsmUtils(getAsmModel().getResourceSet());
        toDependencySortedDeleteStatementStream(deleteStatements, deleteRdbmsReferences)
                .forEach(consumer(deleteStatement -> {

                    EClass entity = deleteStatement.getInstance().getType();
                    ID identifier = deleteStatement.getInstance().getIdentifier();

                    // Collecting all tables on the inheritance chain to delete.
                    Set<EClass> types = asmUtils.all(EClass.class)
                                                .filter(c -> AsmUtils.isEntityType(c) && (entity.isSuperTypeOf(c) || c.isSuperTypeOf(entity))) // subtypes + supertypes
                                                .collect(Collectors.toSet());
                    types.add(entity);

                    types.stream()
                          .filter(AsmUtils::isEntityType)
                          .forEach(entityForCurrentStatement -> {

                                MapSqlParameterSource deleteStatementParameters = new MapSqlParameterSource()
                                        .addValue(getIdentifierProvider().getName(), getCoercer().coerce(identifier, getRdbmsParameterMapper().getIdClassName()), getRdbmsParameterMapper().getIdSqlType());

                                String tableName = getRdbmsResolver().rdbmsTable(entityForCurrentStatement).getSqlName();
                                String sql = "DELETE FROM " + tableName + " WHERE " + ID_COLUMN_NAME + " = :" + getIdentifierProvider().getName();

                                if (log.isDebugEnabled()) {
                                    log.debug("Delete: " + getClassifierFQName(
                                            entityForCurrentStatement) + " " + tableName +
                                            " ID: " + identifier +
                                            " SQL: " + sql +
                                            " Params: " + ImmutableMap.copyOf(deleteStatementParameters.getValues()).toString());
                                }

                                int count = jdbcTemplate.update(sql, deleteStatementParameters);
                                checkState(count == 0 || count == 1, "Maximum of 1 record should have been deleted. Actual: " + count);
                            });
                }));
    }


    /**
     * Topological Sorting over foreign key dependencies.
     * @param deleteStatements The existing insert statements.
     * @param insertRdbmsReference
     * @return
     */
    private Stream<DeleteStatement<ID>> toDependencySortedDeleteStatementStream(
                            Collection<DeleteStatement<ID>> deleteStatements,
                            Collection<RdbmsReference<ID>> insertRdbmsReference) {

          // Topoligical Sorting over foreign key dependencies
          Graph<Statement<ID>, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
          deleteStatements.stream().forEach(s -> graph.addVertex(s));
          insertRdbmsReference.stream()
                  .filter(rdbmsReference -> getRdbmsResolver().rdbmsField(rdbmsReference.getReference()).isMandatory())
                  .forEach(rdbmsReference -> {

                      Statement<ID> oppositeStatement = deleteStatements.stream()
                              .filter(insertStatement ->
                                      insertStatement
                                              .getInstance()
                                              .getIdentifier()
                                              .equals(rdbmsReference.getOppositeIdentifier()))
                              .findFirst()
                              .get();

                      if (rdbmsReference.getRule().isForeignKey()) {
                          graph.addEdge(oppositeStatement, rdbmsReference.getStatement());
                      } else {
                          graph.addEdge(rdbmsReference.getStatement(), oppositeStatement);
                      }
                  });

          // Iterate the ordered statement
          @SuppressWarnings({ "rawtypes", "unchecked" })
          Iterator<DeleteStatement<ID>> iterator = new TopologicalOrderIterator(graph);
          return stream(spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }


    /**
     * Extracting reference informations from {@link InsertStatement} instances. It is used to resolve cross
     * references between {@link DeleteStatement} statements to be able to make topological sort.
     *
     * @param deleteStatements
     * @param removeReferenceStatements
     * @return
     */
    private Set<RdbmsReference<ID>> toRdbmsReferences(Collection<DeleteStatement<ID>> deleteStatements,
                                                      Collection<RemoveReferenceStatement<ID>> removeReferenceStatements) {
        return deleteStatements.stream()
                .flatMap(deleteStatement -> removeReferenceStatements.stream()
                        .filter(removeReferenceStatement ->
                                removeReferenceStatement.getInstance().getIdentifier().equals(deleteStatement.getInstance().getIdentifier())
                        )
                        .map(addReferenceStatement ->
                                {
                                    RdbmsReference<ID> rdbmsReference =  rdbmsReferenceUtil.buildRdbmsReferenceForStatement(
                                            RdbmsReference.<ID>rdbmsReferenceBuilder()
                                                    .statement(deleteStatement)
                                                    .identifier(deleteStatement.getInstance().getIdentifier())
                                                    .oppositeIdentifier(addReferenceStatement.getIdentifier())
                                                    .reference(addReferenceStatement.getReference())
                                    );
                                    return rdbmsReference;
                                }
                                ))
                .filter(rdbmsReference -> rdbmsReference.getRule().isForeignKey()
                        || rdbmsReference.getRule().isInverseForeignKey())
                .filter(t -> {
                    if (log.isDebugEnabled()) log.debug("Delete Statement RdbmsReference: " + rdbmsReferenceUtil.toString(t));
                    return true;
                })
                .collect(Collectors.toSet());
    }
}
