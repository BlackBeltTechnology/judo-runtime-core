package hu.blackbelt.judo.runtime.core.dao.rdbms.executors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.core.statements.UpdateStatement;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.getClassifierFQName;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.isEntityType;
import static org.jooq.lambda.Unchecked.consumer;

/**
 * Executing {@link UpdateStatement}s.
 * @param <ID>
 */
@Slf4j(topic = "dao-rdbms")
class UpdateStatementExecutor<ID> extends StatementExecutor<ID> {

    public UpdateStatementExecutor(AsmModel asmModel, RdbmsModel rdbmsModel,
                                   TransformationTraceService transformationTraceService,
                                   RdbmsParameterMapper rdbmsParameterMapper, Coercer coercer,
                                   IdentifierProvider<ID> identifierProvider, Dialect dialect) {
        super(asmModel, rdbmsModel, transformationTraceService, rdbmsParameterMapper, coercer, identifierProvider, dialect);
    }

    /**
     * It executes {@link UpdateStatement} instances with dependency order, uses
     * topological order between foreign key dependencies.
     *
     * It makes it in two phase, the first inserts the record with the attributes and mandatory references, in
     * second phase adds optional dependencies.
     *
     * @param jdbcTemplate
     * @param updateStatements
     */
    public void executeUpdateStatements(NamedParameterJdbcTemplate jdbcTemplate,
                                        Collection<UpdateStatement<ID>> updateStatements) {

        RdbmsResolver rdbms = new RdbmsResolver(asmModel, transformationTraceService);

        updateStatements.forEach(consumer(updateStatement -> {

                    EClass entity = updateStatement.getInstance().getType();
                    ID identifier = updateStatement.getInstance().getIdentifier();

                    // Collect columns
                    Map<EAttribute, Object> attributeMap = updateStatement.getInstance().getAttributes().stream()
                            .collect(HashMap::new, (m,v) -> m.put(
                                    v.getAttribute(),
                                    v.getValue()),
                                    HashMap::putAll);

                    // Collecting all tables on the inheritance chain which required to insert.
                    Stream.concat(
                            ImmutableList.of(entity).stream(),
                            entity.getEAllSuperTypes().stream()).filter(t -> isEntityType(t))
                            .forEach(entityForCurrentStatement -> {

                                // Phase 1: Insert all type with mandatory reference fields
                                MapSqlParameterSource updateStatementNamedParameters = new MapSqlParameterSource()
                                        .addValue(identifierProvider.getName(), coercer.coerce(identifier, rdbmsParameterMapper.getIdClassName()), rdbmsParameterMapper.getIdSqlType());


                                Map<EAttribute, Object> attributeMapforCurrentStatement = attributeMap.entrySet().stream()
                                        .filter(e -> e.getKey().eContainer().equals(entityForCurrentStatement))
                                        .collect(HashMap::new, (m,v) -> m.put(
                                                v.getKey(),
                                                v.getValue()),
                                                HashMap::putAll);

                                Integer version = coercer.coerce(updateStatement.getVersion(), Integer.class);
                                if (version != null || updateStatement.getTimestamp() != null || updateStatement.getUserId() != null || updateStatement.getUserName() != null) {
                                    MapSqlParameterSource metaUpdateStatementNamedParameters = new MapSqlParameterSource()
                                            .addValue(identifierProvider.getName(), coercer.coerce(identifier, rdbmsParameterMapper.getIdClassName()), rdbmsParameterMapper.getIdSqlType());

                                    if (version != null) {
                                        metaUpdateStatementNamedParameters.addValue(ENTITY_VERSION_MAP_KEY, version, Types.INTEGER);
                                    }

                                    Map<String, String> fields = new TreeMap<>();

                                    if (updateStatement.getTimestamp() != null) {
                                        fields.put(ENTITY_UPDATE_TIMESTAMP_MAP_KEY, ENTITY_UPDATE_TIMESTAMP_COLUMN_NAME);
                                        metaUpdateStatementNamedParameters.addValue(ENTITY_UPDATE_TIMESTAMP_MAP_KEY, coercer.coerce(updateStatement.getTimestamp(), OffsetDateTime.class), Types.TIMESTAMP_WITH_TIMEZONE);
                                    }
                                    if (updateStatement.getUserId() != null) {
                                        fields.put(ENTITY_UPDATE_USER_ID_MAP_KEY, ENTITY_UPDATE_USER_ID_COLUMN_NAME);
                                        metaUpdateStatementNamedParameters.addValue(ENTITY_UPDATE_USER_ID_MAP_KEY, coercer.coerce(updateStatement.getUserId(), identifierProvider.getType()), rdbmsParameterMapper.getSqlType(identifierProvider.getType().getName()));
                                    }
                                    if (updateStatement.getUserName() != null) {
                                        fields.put(ENTITY_UPDATE_USERNAME_MAP_KEY, ENTITY_UPDATE_USERNAME_COLUMN_NAME);
                                        metaUpdateStatementNamedParameters.addValue(ENTITY_UPDATE_USERNAME_MAP_KEY, coercer.coerce(updateStatement.getUserName(), String.class), Types.VARCHAR);
                                    }

                                    String tableName = rdbms.rdbmsTable(entityForCurrentStatement).getSqlName();
                                    String sql = "UPDATE  " + tableName + " SET " + ENTITY_VERSION_COLUMN_NAME + " = " + ENTITY_VERSION_COLUMN_NAME + " + 1" +
                                            (!fields.isEmpty() ? fields.entrySet().stream().map(e -> ", " + e.getValue() + " = :" + e.getKey()).collect(Collectors.joining()) : "") +
                                            " WHERE " + ID_COLUMN_NAME + " = :" + identifierProvider.getName() +
                                            (version != null ? " AND " + ENTITY_VERSION_COLUMN_NAME + " = :" + ENTITY_VERSION_MAP_KEY : "");

                                    log.debug("Update: " + getClassifierFQName(entityForCurrentStatement) + " " + tableName +
                                            " ID: " + identifier +
                                            " SQL: " + sql +
                                            " Params: " + metaUpdateStatementNamedParameters.getValues());

                                    int count = jdbcTemplate.update(sql, metaUpdateStatementNamedParameters);
                                    checkState(count == 1, "There is illegal state, no records updated");
                                }

                                if (attributeMapforCurrentStatement.size() > 0) {

                                    rdbms.logAttributeParameters(attributeMapforCurrentStatement);

                                    rdbmsParameterMapper.mapAttributeParameters(updateStatementNamedParameters, attributeMapforCurrentStatement);

                                    String tableName = rdbms.rdbmsTable(entityForCurrentStatement).getSqlName();

                                    Map<String, String> fields =
                                            attributeMapforCurrentStatement.entrySet().stream()
                                                    .collect(Collectors.toMap(
                                                            e -> e.getKey().getName(),
                                                            e -> rdbms.rdbmsField(e.getKey()).getSqlName()
                                                    ));

                                    String sql = "UPDATE  " + tableName + " SET " +
                                            fields.entrySet().stream().map(e -> e.getValue() + " = :" + e.getKey()).collect(Collectors.joining(", ")) +
                                            " WHERE " + ID_COLUMN_NAME + " = :" + identifierProvider.getName();
                                    Map<String, Object> paramNullReplaced = updateStatementNamedParameters.getValues().entrySet()
                                                    .stream()
                                                    .collect(
                                                            HashMap::new,
                                                            (m,v) -> m.put(
                                                                    v.getKey(),
                                                                    v.getValue() == null ? "<NULL>" : v.getValue()),
                                                            HashMap::putAll
                                                    );


                                    log.debug("Update: " + getClassifierFQName(entityForCurrentStatement) + " " + tableName +
                                            " ID: " + identifier +
                                            " SQL: " + sql +
                                            " Params: " + ImmutableMap.copyOf(paramNullReplaced).toString());

                                    int count = jdbcTemplate.update(sql, updateStatementNamedParameters);
                                    checkState(count == 1, "There is illegal state, no records updated");
                                }
                            });
                }));
    }
}
