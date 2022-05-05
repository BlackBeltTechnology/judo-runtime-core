package hu.blackbelt.judo.services.dao.rdbms.executors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.services.dao.core.statements.AddReferenceStatement;
import hu.blackbelt.judo.services.dao.core.statements.InsertStatement;
import hu.blackbelt.judo.services.dao.core.statements.Statement;
import hu.blackbelt.judo.services.dao.rdbms.Dialect;
import hu.blackbelt.judo.services.dao.rdbms.RdbmsReference;
import hu.blackbelt.judo.services.dao.rdbms.RdbmsReferenceUtil;
import hu.blackbelt.judo.services.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.getClassifierFQName;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.isEntityType;
import static hu.blackbelt.judo.services.dao.rdbms.RdbmsReference.rdbmsReferenceBuilder;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;
import static org.jooq.lambda.Unchecked.consumer;

/**
 * Executing {@link InsertStatement}s. It have use {@link AddReferenceStatement} instances too because some of them
 * are embedded the insert statements directly.
 *
 * @param <ID>
 */
@Slf4j(topic = "dao-rdbms")
class InsertStatementExecutor<ID> extends StatementExecutor<ID> {

    private final RdbmsReferenceUtil<ID> rdbmsReferenceUtil;

    public InsertStatementExecutor(AsmModel asmModel, RdbmsModel rdbmsModel,
                                   TransformationTraceService transformationTraceService, Coercer coercer,
                                   IdentifierProvider<ID> identifierProvider,
                                   Dialect dialect) {
        super(asmModel, rdbmsModel, transformationTraceService, coercer, identifierProvider, dialect);
        rdbmsReferenceUtil = new RdbmsReferenceUtil<>(asmModel, rdbmsModel, transformationTraceService);
    }

    /**
     * It executes {@link InsertStatement} instances with dependency order, uses
     * topological order between foreign key dependencies.
     *
     * It makes it in two phase, the first inserts the record with the attributes and mandatory references, in
     * second phase adds optional dependencies.
     *
     * @param jdbcTemplate
     * @param insertStatements
     * @param addReferenceStatements
     */
    public void executeInsertStatements(NamedParameterJdbcTemplate jdbcTemplate,
                                        Collection<InsertStatement<ID>> insertStatements,
                                        Collection<AddReferenceStatement<ID>> addReferenceStatements) {

        RdbmsResolver rdbms = new RdbmsResolver(asmModel, transformationTraceService);

        // Collect all information required to build dependencies between nodes.
        Set<RdbmsReference<ID>> insertRdbmsReferences = toRdbmsReferences(insertStatements, addReferenceStatements);

        toDependencySortedInsertStatementStream(insertStatements, insertRdbmsReferences)
                .forEach(consumer(insertStatement -> {

                    EClass entity = insertStatement.getInstance().getType();
                    ID identifier = insertStatement.getInstance().getIdentifier();

                    // Collect columns
                    Map<EAttribute, Object> attributeMap = insertStatement.getInstance().getAttributes().stream()
                            .filter(a -> a.getValue() != null)
                            .collect(HashMap::new, (m,v) -> m.put(
                                    v.getAttribute(),
                                    v.getValue()),
                                    HashMap::putAll);

                    // Mandatory fields are updated only here, because there is no guarentee that the
                    // referenced element already inserted.
                    // There is some optimalization point that optional fields also can be inserted when
                    // it is not in the inserted statements
                    Map<RdbmsReference<ID>, ID> mandatoryReferenceMap =
                            collectReferenceIdentifiersForGivenIdentifier(
                                    insertStatement.getInstance().getIdentifier(),
                                    ImmutableList.copyOf(addReferenceStatements),
                                    true,
                                    false);

                    // Collecting all tables on the inheritance chain which required to insert.
                    Stream.concat(
                            ImmutableList.of(entity).stream(),
                            entity.getEAllSuperTypes().stream()).filter(t -> isEntityType(t))
                            .forEach(entityForCurrentStatement -> {
                                // Phase 1: Insert all type with mandatory reference fields
                                MapSqlParameterSource insertStatementNamedParameters = new MapSqlParameterSource()
                                        .addValue(identifierProvider.getName(), coercer.coerce(identifier, parameterMapper.getIdClassName()), parameterMapper.getIdSqlType())
                                        .addValue(ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(entity), Types.VARCHAR);

                                Map<String, String> metaMapping = new TreeMap<>();
                                metaMapping.put(identifierProvider.getName(), ID_COLUMN_NAME);
                                metaMapping.put(ENTITY_TYPE_MAP_KEY, ENTITY_TYPE_COLUMN_NAME);

                                if (insertStatement.getVersion() != null) {
                                    insertStatementNamedParameters.addValue(ENTITY_VERSION_MAP_KEY, coercer.coerce(insertStatement.getVersion(), Integer.class), Types.INTEGER);
                                    metaMapping.put(ENTITY_VERSION_MAP_KEY, ENTITY_VERSION_COLUMN_NAME);
                                }
                                if (insertStatement.getTimestamp() != null) {
                                    insertStatementNamedParameters.addValue(ENTITY_CREATE_TIMESTAMP_MAP_KEY, coercer.coerce(insertStatement.getTimestamp(), OffsetDateTime.class), Types.TIMESTAMP_WITH_TIMEZONE);
                                    metaMapping.put(ENTITY_CREATE_TIMESTAMP_MAP_KEY, ENTITY_CREATE_TIMESTAMP_COLUMN_NAME);
                                }
                                if (insertStatement.getUserId() != null) {
                                    insertStatementNamedParameters.addValue(ENTITY_CREATE_USER_ID_MAP_KEY, coercer.coerce(insertStatement.getUserId(), identifierProvider.getType()), parameterMapper.getSqlType(identifierProvider.getType().getName()));
                                    metaMapping.put(ENTITY_CREATE_USER_ID_MAP_KEY, ENTITY_CREATE_USER_ID_COLUMN_NAME);
                                }
                                if (insertStatement.getUserName() != null) {
                                    insertStatementNamedParameters.addValue(ENTITY_CREATE_USERNAME_MAP_KEY, coercer.coerce(insertStatement.getUserName(), String.class), Types.VARCHAR);
                                    metaMapping.put(ENTITY_CREATE_USERNAME_MAP_KEY, ENTITY_CREATE_USERNAME_COLUMN_NAME);
                                }

                                Map<EAttribute, Object> attributeMapforCurrentStatement = attributeMap.entrySet().stream()
                                        .filter(e -> e.getKey().eContainer().equals(entityForCurrentStatement))
                                        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

                                Map<EReference, Object> mandatoryReferenceMapForCurrentStatement = mandatoryReferenceMap.entrySet().stream()
                                        .filter(e -> (e.getKey().getReference().eContainer().equals(entityForCurrentStatement)) ||
                                                (e.getKey().getReference().getEReferenceType().equals(entityForCurrentStatement)))
                                        .collect(toMap(e -> e.getKey().getReference(), Map.Entry::getValue));

                                rdbms.logAttributeParameters(attributeMapforCurrentStatement);
                                parameterMapper.mapAttributeParameters(insertStatementNamedParameters, attributeMapforCurrentStatement);
                                rdbms.logReferenceParameters(mandatoryReferenceMapForCurrentStatement);
                                parameterMapper.mapReferenceParameters(insertStatementNamedParameters, mandatoryReferenceMapForCurrentStatement);

                                String tableName = rdbms.rdbmsTable(entityForCurrentStatement).getSqlName();

                                /*
                                Map<String, String> fields = Stream.concat(ImmutableMap.of(
                                        ID_COLUMN_NAME, ":" + ID_MAP_KEY).entrySet().stream(),
                                        Stream.concat(
                                            attributeMapforCurrentStatement.keySet().stream().collect(toMap(
                                                    e -> rdbms.rdbmsField(e).getName(),
                                                    e -> rdbms.rdbmsField(e).getSqlName()
                                            )).entrySet().stream(),

                                        mandatoryReferenceMapForCurrentStatement.keySet().stream().collect(toMap(
                                                e -> rdbms.rdbmsField(e).getName(),
                                                e -> rdbms.rdbmsField(e).getSqlName()
                                        )).entrySet().stream()
                                )).collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
                                */

                                /*
                                Map<String, String> fields = Stream.concat(ImmutableMap.of(
                                        ID_COLUMN_NAME, ID_MAP_KEY).entrySet().stream(),
                                        Stream.concat(
                                                attributeMapforCurrentStatement.keySet().stream()
                                                    .map(e -> rdbms.rdbmsField(e)),
                                                mandatoryReferenceMapForCurrentStatement.keySet().stream()
                                                    .map(e -> rdbms.rdbmsField(e))
                                        ).collect(toMap(e -> e.getName(), e -> e.getSqlName())).entrySet().stream()
                                ).collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

                                 */

                                Map<String, String> fields = Stream.concat(
                                        metaMapping.entrySet().stream(),
                                        Stream.concat(
                                                attributeMapforCurrentStatement.keySet().stream()
                                                        .collect(toMap(
                                                                e -> e.getName(),
                                                                e -> rdbms.rdbmsField(e).getSqlName()))
                                                        .entrySet().stream(),
                                                mandatoryReferenceMapForCurrentStatement.keySet().stream()
                                                        .collect(toMap(
                                                                e -> e.getName(),
                                                                e -> rdbms.rdbmsField(e).getSqlName()))
                                                        .entrySet().stream()
                                        )
                                ).collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

                                String sql = "INSERT INTO " + tableName + "(" +
                                        fields.entrySet().stream().map(e -> e.getValue()).collect(joining(", ")) +
                                        ") VALUES ( " +
                                        fields.entrySet().stream().map(e -> ":" + e.getKey()).collect(joining(", ")) +
                                        ")";

                                log.debug("Insert: " + getClassifierFQName(entityForCurrentStatement) + " " + tableName +
                                        " ID: " + identifier +
                                        " SQL: " + sql +
                                        " Params: " + ImmutableMap.copyOf(insertStatementNamedParameters.getValues()).toString());

                                int count = jdbcTemplate.update(sql, insertStatementNamedParameters);
                                checkState(count == 1, "There is illegal state, no records inserted");
                            });
                }));
    }


    /**
     * Topological Sorting over foreign key dependencies.
     * @param insertStatements The existing insert statements.
     * @param rdbmsReferences
     * @return
     */
    private Stream<InsertStatement<ID>> toDependencySortedInsertStatementStream(
                            Collection<InsertStatement<ID>> insertStatements,
                            Collection<RdbmsReference<ID>> rdbmsReferences) {
          RdbmsResolver rdbms = new RdbmsResolver(asmModel, transformationTraceService);

          // Topoligical Sorting over foreign key dependencies
          Graph<Statement, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
          insertStatements.stream().forEach(s -> graph.addVertex(s));
          rdbmsReferences.stream()
                  .filter(rdbmsReference -> rdbms.rdbmsField(rdbmsReference.getReference()).isMandatory())
                  .forEach(rdbmsReference -> {

                      Statement oppositeStatement = insertStatements.stream()
                              .filter(insertStatement ->
                                      insertStatement
                                              .getInstance()
                                              .getIdentifier()
                                              .equals(rdbmsReference.getOppositeIdentifier()))
                              .findFirst()
                              .get();

                      if (rdbmsReference.getRule().isForeignKey()) {
                          graph.addEdge(rdbmsReference.getStatement(), oppositeStatement);
                      } else {
                          graph.addEdge(oppositeStatement, rdbmsReference.getStatement());
                      }
                  });

          // Iterate the ordered statement
          Iterator<InsertStatement<ID>> iterator = new TopologicalOrderIterator(graph);
          return stream(spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }


    /**
     * Extracting reference information from {@link InsertStatement} instances. It is used to resolve cross
     * references between {@link InsertStatement} statements to be able to make topological sort.
     *
     * @param insertStatements
     * @param addReferenceStatements
     * @return
     */
    private Set<RdbmsReference<ID>> toRdbmsReferences(Collection<InsertStatement<ID>> insertStatements,
                                                      Collection<AddReferenceStatement<ID>> addReferenceStatements) {
        return insertStatements.stream()
                .flatMap(insertStatement -> addReferenceStatements.stream()
                        .filter(addReferenceStatement ->
                                addReferenceStatement.getInstance().getIdentifier().equals(insertStatement.getInstance().getIdentifier())
                        )
                        .map(addReferenceStatement ->
                                {
                                    RdbmsReference<ID> rdbmsReference =  rdbmsReferenceUtil.buildRdbmsReferenceForStatement(
                                            rdbmsReferenceBuilder()
                                                    .statement(insertStatement)
                                                    .identifier(insertStatement.getInstance().getIdentifier())
                                                    .oppositeIdentifier(addReferenceStatement.getIdentifier())
                                                    .reference(addReferenceStatement.getReference())
                                    );
                                    return rdbmsReference;
                                }
                                ))
                .filter(rdbmsReference -> rdbmsReference.getRule().isForeignKey()
                        || rdbmsReference.getRule().isInverseForeignKey())
                .filter(t -> {
                    if (log.isDebugEnabled()) log.debug("Insert Statement RdbmsReference: " + rdbmsReferenceUtil.toString(t));
                    return true;
                })
                .collect(Collectors.toSet());
    }
}
