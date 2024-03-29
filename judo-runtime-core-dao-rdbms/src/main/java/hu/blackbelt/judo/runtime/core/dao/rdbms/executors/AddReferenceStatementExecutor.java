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
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.rdbms.RdbmsField;
import hu.blackbelt.judo.meta.rdbms.RdbmsTable;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.core.statements.AddReferenceStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.ReferenceStatement;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsReference;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.getClassifierFQName;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.getReferenceFQName;
import static java.util.stream.Stream.*;

/**
 * Executing {@link AddReferenceStatement} statements.
 * It analyzes where the foreign key are presented and making update in the owner table. If it is a join table
 * record is created.
 *
 * @param <ID>
 */
@Slf4j(topic = "dao-rdbms")
class AddReferenceStatementExecutor<ID> extends StatementExecutor<ID> {

    @Builder
    public AddReferenceStatementExecutor(
            @NonNull AsmModel asmModel,
            @NonNull RdbmsModel rdbmsModel,
            @NonNull TransformationTraceService transformationTraceService,
            @NonNull RdbmsParameterMapper<ID> rdbmsParameterMapper,
            @NonNull RdbmsResolver rdbmsResolver,
            @NonNull Coercer coercer,
            @NonNull IdentifierProvider<ID> identifierProvider) {
        super(asmModel, rdbmsModel, transformationTraceService, rdbmsParameterMapper, rdbmsResolver, coercer, identifierProvider);
    }

    /**
     * Collecting {@link AddReferenceStatement} statements from statement list and execute it.
     * @param jdbcTemplate
     * @param statements
     * @throws SQLException
     */
    public void executeAddReferenceStatements(NamedParameterJdbcTemplate jdbcTemplate,
                                              Collection<ReferenceStatement<ID>> statements) {

        Map<ID, EClass> statementBased = statements.stream()
                .collect(Collectors.toMap(
                        k -> k.getIdentifier(),
                        v -> v.getReference().getEContainingClass(),
                        (v1, v2) -> v1));

        Map<ID, EClass> instanceBased = statements.stream()
                .collect(Collectors.toMap(
                        k -> k.getInstance().getIdentifier(),
                        v -> v.getInstance().getType(),
                        (v1, v2) -> v1));

        Map<ID, EClass> classById = concat(statementBased.entrySet().stream(), instanceBased.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry :: getKey,
                        Map.Entry :: getValue,
                        (v1, v2) -> v1));

        Map<ID, Map<RdbmsReference<ID>, ID>> referenceMap = classById.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry :: getKey,
                        e -> collectReferenceIdentifiersForGivenIdentifier(
                                e.getKey(),
                                ImmutableList.copyOf(statements),
                                false,
                                true)
                ));

        // TODO: Merge update statements referencing to One table and identifier
        referenceMap.entrySet().stream().forEach(idAndRdbmsReferenceMap -> {

            idAndRdbmsReferenceMap.getValue().entrySet().forEach(rdbmsReferenceAndOppositeId -> {

                EReference reference = rdbmsReferenceAndOppositeId.getKey().getReference();
                concat(ImmutableList.of(reference.getEContainingClass()).stream(),
                        reference.getEContainingClass().getEAllSuperTypes().stream())
                        .filter(r -> r.equals(reference.getEReferenceType()) ||
                                r.equals(reference.getEContainingClass()))
                        .forEach(entityForCurrentStatement -> {
                            Optional<ID> identifier = Optional.empty();
                            Optional<ID> referenceIdentifier = Optional.empty();
                            Optional<EClass> entity = Optional.empty();

                            if (rdbmsReferenceAndOppositeId.getKey().getRule().isForeignKey()) {
                                entity = Optional.of(reference.getEContainingClass());
                            } else {
                                entity = Optional.of(reference.getEReferenceType());
                            }

                            if (entity.isPresent()) {
                                if (rdbmsReferenceAndOppositeId.getKey().getReference().getEContainingClass().equals(entity)) {
                                    referenceIdentifier = Optional.of(idAndRdbmsReferenceMap.getKey());
                                    identifier = Optional.of(rdbmsReferenceAndOppositeId.getValue());
                                } else {
                                    identifier = Optional.of(idAndRdbmsReferenceMap.getKey());
                                    referenceIdentifier = Optional.of(rdbmsReferenceAndOppositeId.getValue());
                                }

                                MapSqlParameterSource updateStatementNamedParameters =
                                        new MapSqlParameterSource().addValue(
                                                getIdentifierProvider().getName(),
                                                getCoercer().coerce(identifier.get(), getRdbmsParameterMapper().getIdClassName()),
                                                getRdbmsParameterMapper().getIdSqlType());

                                updateStatementNamedParameters.addValue(
                                        getReferenceFQName(reference),
                                        getCoercer().coerce(referenceIdentifier.get(), getRdbmsParameterMapper().getIdClassName()),
                                        getRdbmsParameterMapper().getIdSqlType());

                                String tableName = getRdbmsResolver().rdbmsTable(entity.get()).getSqlName();

                                String sql =
                                        "UPDATE " + tableName + " SET " + getRdbmsResolver().rdbmsField(reference).getSqlName() +
                                                " = :" + getReferenceFQName(reference)
                                                + " WHERE " + ID_COLUMN_NAME + " = :" + getIdentifierProvider().getName();

                                if (log.isDebugEnabled()) {
                                    log.debug("Add reference: " + getClassifierFQName(
                                            entityForCurrentStatement) + " " + tableName +
                                            " ID: " + identifier.get() +
                                            " Reference ID: " + referenceIdentifier.get() +
                                            " SQL: " + sql +
                                            " Params: " + ImmutableMap.copyOf(updateStatementNamedParameters.getValues()).toString());
                                }

                                int count = jdbcTemplate.update(sql, updateStatementNamedParameters);
                                checkState(count == 1, "There is illegal state, no records updated on delete reference");
                            }
                        });
            });
        });

        // Join reference
        collectRdbmsReferencesReferenceStatements(statements)
                .filter(r ->
                        r.getRule().isJoinTable() &&
                                // Just one side required
                                r.getReference().equals(((ReferenceStatement<ID>) r.getStatement()).getReference())
                )
                .forEach(r -> {

                    RdbmsTable joinTable = getRdbmsResolver().rdbmsJunctionTable(r.getReference());
                    RdbmsField aFk = getRdbmsResolver().rdbmsJunctionOppositeField(r.getReference());
                    RdbmsField bFk = getRdbmsResolver().rdbmsJunctionField(r.getReference());
                    ID aId = r.getIdentifier();
                    ID bId = r.getOppositeIdentifier();

                    SqlParameterSource jointPairNamedParameters = new MapSqlParameterSource()
                            .addValue("id", getCoercer().coerce(UUID.randomUUID(), getRdbmsParameterMapper().getIdClassName()), getRdbmsParameterMapper().getIdSqlType())
                            .addValue("aId", getCoercer().coerce(aId, getRdbmsParameterMapper().getIdClassName()), getRdbmsParameterMapper().getIdSqlType())
                            .addValue("bId", getCoercer().coerce(bId, getRdbmsParameterMapper().getIdClassName()), getRdbmsParameterMapper().getIdSqlType());

                    // Check existence of the given ID pair.
                    String exitenceCheckSql = "SELECT count(1) FROM " + joinTable.getSqlName() + " WHERE " +
                            aFk.getSqlName() + " = :aId AND " +
                            bFk.getSqlName() + " = :bId";

                    if (log.isDebugEnabled()) {
                        log.debug("JoinTableExistsCheck: " + AsmUtils.getClassifierFQName(
                                r.getStatement().getInstance().getType()) +
                                " " + joinTable.getSqlName() + " A ID: " + aId +
                                " B ID: " + bId + " SQL: " + exitenceCheckSql);
                    }

                    int count = jdbcTemplate.queryForObject(exitenceCheckSql, jointPairNamedParameters, Integer.class);

                    checkState(count == 0,
                            String.format("The given reference %s (%s) with identifier %s and identifier %s already exists.",
                                    r.getStatement().getInstance().getType(),
                                    joinTable.getSqlName(),
                                    aId,
                                    bId));

                    // Insert reference
                    String insertJoinTableSql = "INSERT INTO " + joinTable.getSqlName() + "(" + "ID, " +
                            aFk.getSqlName() + ", " + bFk.getSqlName() +
                            ") VALUES (" +
                            ":id, :aId, :bId)";

                    if (log.isDebugEnabled()) {
                        log.debug("Add reference: " + AsmUtils.getClassifierFQName(
                                r.getStatement().getInstance().getType()) +
                                " " + joinTable.getSqlName() + " A ID: " + aId +
                                " B ID: " + bId + " SQL: " + exitenceCheckSql);
                    }

                    count = jdbcTemplate.update(insertJoinTableSql, jointPairNamedParameters);
                    checkState(count == 1, "There is illegal state, no records updated on delete reference");
                });
    }
}
