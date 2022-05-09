package hu.blackbelt.judo.runtime.core.dao.rdbms.executors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.core.statements.DeleteStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.InsertStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.RemoveReferenceStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.Statement;
import hu.blackbelt.judo.runtime.core.dao.rdbms.*;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.getClassifierFQName;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.isEntityType;
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
    public DeleteStatementExecutor(AsmModel asmModel, RdbmsModel rdbmsModel,
                                   TransformationTraceService transformationTraceService,
                                   RdbmsParameterMapper rdbmsParameterMapper, Coercer coercer,
                                   IdentifierProvider<ID> identifierProvider, Dialect dialect) {
        super(asmModel, rdbmsModel, transformationTraceService, rdbmsParameterMapper, coercer, identifierProvider, dialect);
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

        RdbmsResolver rdbms = new RdbmsResolver(asmModel, transformationTraceService);

        // Collect all information required to build dependencies between nodes.
        Set<RdbmsReference<ID>> deleteRdbmsReferences = toRdbmsReferences(deleteStatements, removeReferenceStatements);

        toDependencySortedDeleteStatementStream(deleteStatements, deleteRdbmsReferences)
                .forEach(consumer(deleteStatement -> {

                    EClass entity = deleteStatement.getInstance().getType();
                    ID identifier = deleteStatement.getInstance().getIdentifier();

                    // Collecting all tables on the inheritance chain which required to insert.
                    Stream.concat(
                            ImmutableList.of(entity).stream(),
                            entity.getEAllSuperTypes().stream()).filter(t -> isEntityType(t))
                            .forEach(entityForCurrentStatement -> {

                                MapSqlParameterSource deleteStatementParameters = new MapSqlParameterSource()
                                        .addValue(identifierProvider.getName(), coercer.coerce(identifier, rdbmsParameterMapper.getIdClassName()), rdbmsParameterMapper.getIdSqlType());

                                String tableName = rdbms.rdbmsTable(entityForCurrentStatement).getSqlName();
                                String sql = "DELETE FROM " + tableName + " WHERE " + ID_COLUMN_NAME + " = :" + identifierProvider.getName();

                                log.debug("Delete: " + getClassifierFQName(
                                        entityForCurrentStatement) + " " + tableName +
                                        " ID: " + identifier +
                                        " SQL: " + sql +
                                        " Params: " + ImmutableMap.copyOf(deleteStatementParameters.getValues()).toString());

                                int count = jdbcTemplate.update(sql, deleteStatementParameters);
                                checkState(count == 1, "There is illegal state, no records deleted");
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
          RdbmsResolver rdbms = new RdbmsResolver(asmModel, transformationTraceService);

          // Topoligical Sorting over foreign key dependencies
          Graph<Statement, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
          deleteStatements.stream().forEach(s -> graph.addVertex(s));
          insertRdbmsReference.stream()
                  .filter(rdbmsReference -> rdbms.rdbmsField(rdbmsReference.getReference()).isMandatory())
                  .forEach(rdbmsReference -> {

                      Statement oppositeStatement = deleteStatements.stream()
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
                                            RdbmsReference.rdbmsReferenceBuilder()
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
