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
import hu.blackbelt.judo.runtime.core.dao.core.statements.AddReferenceStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.RemoveReferenceStatement;
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

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.getClassifierFQName;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.isEntityType;
import static java.util.stream.Collectors.toMap;
import static org.jooq.lambda.Unchecked.consumer;

/**
 * Executing {@link AddReferenceStatement} and {@link RemoveReferenceStatement}s with same entity ID as single a single update.
 * @param <ID>
 */
@Slf4j(topic = "dao-rdbms")
class UpdateReferenceExecutor<ID> extends StatementExecutor<ID> {

    @Builder
    public UpdateReferenceExecutor(
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
     * It executes {@link AddReferenceStatement} and  {@link RemoveReferenceStatement} instances where same ID is used to
     * update reference.
     *
     * @param jdbcTemplate
     * @param updateReferenceStatements
     */
    public void executeReferenceUpdateStatements(NamedParameterJdbcTemplate jdbcTemplate,
                                        Collection<AddReferenceStatement<ID>> updateReferenceStatements
                                        ) {


        updateReferenceStatements.forEach(consumer(updateStatement -> {

                    EClass entity = updateStatement.getReference().getEContainingClass();
                    ID identifier = updateStatement.getIdentifier();

                    Map<RdbmsReference<ID>, ID> updateReferenceMap =
                            collectReferenceIdentifiersForGivenIdentifier(
                                    updateStatement.getIdentifier(),
                                    ImmutableList.copyOf(updateReferenceStatements),
                                    true,
                                    true);

                    // Collecting all tables on the inheritance chain which required to insert.
                    Stream.concat(
                            ImmutableList.of(entity).stream(),
                            entity.getEAllSuperTypes().stream()).filter(t -> isEntityType(t))
                            .forEach(entityForCurrentStatement -> {

                                MapSqlParameterSource updateStatementNamedParameters = new MapSqlParameterSource()
                                        .addValue(getIdentifierProvider().getName(), getCoercer().coerce(identifier, getRdbmsParameterMapper().getIdClassName()), getRdbmsParameterMapper().getIdSqlType());

                                Map<EReference, ID> updateReferenceMapForCurrentStatement = updateReferenceMap.entrySet().stream()
                                        .filter(e -> (e.getKey().getReference().eContainer().equals(entityForCurrentStatement)) ||
                                                (e.getKey().getReference().getEReferenceType().equals(entityForCurrentStatement)))
                                        .collect(toMap(e -> e.getKey().getReference(), Map.Entry::getValue));

                                getRdbmsResolver().logReferenceParameters(updateReferenceMapForCurrentStatement);
                                getRdbmsParameterMapper().mapReferenceParameters(updateStatementNamedParameters, updateReferenceMapForCurrentStatement);

                                Map<String, String> fields =
                                                updateReferenceMapForCurrentStatement.keySet().stream()
                                                        .collect(toMap(
                                                                e -> e.getName(),
                                                                e -> getRdbmsResolver().rdbmsField(e).getSqlName()))
                                                        .entrySet().stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

                                if (fields.size() > 0) {

                                    String tableName = getRdbmsResolver().rdbmsTable(entityForCurrentStatement).getSqlName();

                                    String sql = "UPDATE  " + tableName + " SET " +
                                            fields.entrySet().stream().map(e -> e.getValue() + " = :" + e.getKey()).collect(Collectors.joining(", ")) +
                                            " WHERE " + ID_COLUMN_NAME + " = :" + getIdentifierProvider().getName();

                                    log.debug("Update: " + getClassifierFQName(entityForCurrentStatement) + " " + tableName +
                                            " ID: " + identifier +
                                            " SQL: " + sql +
                                            " Params: " + ImmutableMap.copyOf(updateStatementNamedParameters.getValues()).toString());

                                    int count = jdbcTemplate.update(sql, updateStatementNamedParameters);
                                    checkState(count == 1, "There is illegal state, no records updated");
                                }
                            });
                }));
    }
}
