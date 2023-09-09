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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.core.statements.UpdateStatement;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Types;
import java.time.LocalDateTime;
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

    @Builder
    public UpdateStatementExecutor(
            @NonNull AsmModel asmModel,
            @NonNull RdbmsModel rdbmsModel,
            @NonNull TransformationTraceService transformationTraceService,
            @NonNull RdbmsParameterMapper<ID> rdbmsParameterMapper,
            @NonNull RdbmsResolver rdbmsResolver,
            @NonNull Coercer coercer,
            IdentifierProvider<ID> identifierProvider) {
        super(asmModel, rdbmsModel, transformationTraceService, rdbmsParameterMapper, rdbmsResolver, coercer, identifierProvider);
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
                                        .addValue(getIdentifierProvider().getName(), getCoercer().coerce(identifier, getRdbmsParameterMapper().getIdClassName()), getRdbmsParameterMapper().getIdSqlType());


                                Map<EAttribute, Object> attributeMapforCurrentStatement = attributeMap.entrySet().stream()
                                        .filter(e -> e.getKey().eContainer().equals(entityForCurrentStatement))
                                        .collect(HashMap::new, (m,v) -> m.put(
                                                v.getKey(),
                                                v.getValue()),
                                                HashMap::putAll);

                                Integer version = getCoercer().coerce(updateStatement.getVersion(), Integer.class);
                                if (version != null || updateStatement.getTimestamp() != null || updateStatement.getUserId() != null || updateStatement.getUserName() != null) {
                                    MapSqlParameterSource metaUpdateStatementNamedParameters = new MapSqlParameterSource()
                                            .addValue(getIdentifierProvider().getName(), getCoercer().coerce(identifier, getRdbmsParameterMapper().getIdClassName()), getRdbmsParameterMapper().getIdSqlType());

                                    if (version != null) {
                                        metaUpdateStatementNamedParameters.addValue(ENTITY_VERSION_MAP_KEY, version, Types.INTEGER);
                                    }

                                    Map<String, String> fields = new TreeMap<>();

                                    if (updateStatement.getTimestamp() != null) {
                                        fields.put(ENTITY_UPDATE_TIMESTAMP_MAP_KEY, ENTITY_UPDATE_TIMESTAMP_COLUMN_NAME);
                                        metaUpdateStatementNamedParameters.addValue(ENTITY_UPDATE_TIMESTAMP_MAP_KEY,
                                                getCoercer().coerce(updateStatement.getTimestamp(), LocalDateTime.class),
                                                Types.TIMESTAMP);
                                    }
                                    if (updateStatement.getUserId() != null) {
                                        fields.put(ENTITY_UPDATE_USER_ID_MAP_KEY, ENTITY_UPDATE_USER_ID_COLUMN_NAME);
                                        metaUpdateStatementNamedParameters.addValue(ENTITY_UPDATE_USER_ID_MAP_KEY,
                                                getCoercer().coerce(updateStatement.getUserId(), getIdentifierProvider().getType()),
                                                getRdbmsParameterMapper().getSqlType(getIdentifierProvider().getType().getName()));
                                    }
                                    if (updateStatement.getUserName() != null) {
                                        fields.put(ENTITY_UPDATE_USERNAME_MAP_KEY, ENTITY_UPDATE_USERNAME_COLUMN_NAME);
                                        metaUpdateStatementNamedParameters.addValue(ENTITY_UPDATE_USERNAME_MAP_KEY,
                                                getCoercer().coerce(updateStatement.getUserName(), String.class),
                                                Types.VARCHAR);
                                    }

                                    String tableName = getRdbmsResolver().rdbmsTable(entityForCurrentStatement).getSqlName();
                                    String sql = "UPDATE  " + tableName + " SET " + ENTITY_VERSION_COLUMN_NAME + " = " + ENTITY_VERSION_COLUMN_NAME + " + 1" +
                                            (!fields.isEmpty() ? fields.entrySet().stream().map(e -> ", " + e.getValue() + " = :" + e.getKey()).collect(Collectors.joining()) : "") +
                                            " WHERE " + ID_COLUMN_NAME + " = :" + getIdentifierProvider().getName() +
                                            (version != null ? " AND " + ENTITY_VERSION_COLUMN_NAME + " = :" + ENTITY_VERSION_MAP_KEY : "");

                                    log.debug("Update: " + getClassifierFQName(entityForCurrentStatement) + " " + tableName +
                                            " ID: " + identifier +
                                            " SQL: " + sql +
                                            " Params: " + metaUpdateStatementNamedParameters.getValues());

                                    int count = ExecutorUtils.documentExecution(() -> jdbcTemplate.update(sql, metaUpdateStatementNamedParameters), sql);
                                    checkState(count == 1, "There is illegal state, no records updated");
                                }

                                if (attributeMapforCurrentStatement.size() > 0) {

                                    getRdbmsResolver().logAttributeParameters(attributeMapforCurrentStatement);

                                    getRdbmsParameterMapper().mapAttributeParameters(updateStatementNamedParameters, attributeMapforCurrentStatement);

                                    String tableName = getRdbmsResolver().rdbmsTable(entityForCurrentStatement).getSqlName();

                                    Map<String, String> fields =
                                            attributeMapforCurrentStatement.entrySet().stream()
                                                    .collect(Collectors.toMap(
                                                            e -> e.getKey().getName(),
                                                            e -> getRdbmsResolver().rdbmsField(e.getKey()).getSqlName()
                                                    ));

                                    String sql = "UPDATE  " + tableName + " SET " +
                                            fields.entrySet().stream().map(e -> e.getValue() + " = :" + e.getKey()).collect(Collectors.joining(", ")) +
                                            " WHERE " + ID_COLUMN_NAME + " = :" + getIdentifierProvider().getName();
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

                                    int count = ExecutorUtils.documentExecution(() -> jdbcTemplate.update(sql, updateStatementNamedParameters), sql);
                                    checkState(count == 1, "There is illegal state, no records updated");
                                }
                            });
                }));
    }
}
