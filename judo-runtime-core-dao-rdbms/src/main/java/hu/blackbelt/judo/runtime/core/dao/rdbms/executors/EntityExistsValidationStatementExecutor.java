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

import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.ValidationResult;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.exception.ValidationException;
import hu.blackbelt.judo.runtime.core.dao.core.statements.InstanceExistsValidationStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.Statement;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.Unchecked;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * Check the given ID exist on the given type. If the record is not presented an {@link IllegalStateException} thrown.
 * @param <ID>
 */
@Slf4j(topic = "dao-rdbms")
class EntityExistsValidationStatementExecutor<ID> extends StatementExecutor<ID> {

    @Builder
    public EntityExistsValidationStatementExecutor(
            @NonNull AsmModel asmModel,
            @NonNull RdbmsModel rdbmsModel,
            @NonNull TransformationTraceService transformationTraceService,
            @NonNull RdbmsParameterMapper<ID> rdbmsParameterMapper,
            @NonNull RdbmsResolver rdbmsResolver,
            @NonNull Coercer coercer,
            @NonNull IdentifierProvider<ID> identifierProvider) {

        super(asmModel, rdbmsModel, transformationTraceService, rdbmsParameterMapper, rdbmsResolver, coercer, identifierProvider);
    }

    public void executeEntityExistsValidationStatements(
            NamedParameterJdbcTemplate jdbcTemplate,
            List<Statement<ID>> statements) {

        statements.stream()
                .filter(InstanceExistsValidationStatement.class :: isInstance)
                .map(o -> (InstanceExistsValidationStatement<ID>) o)
                .forEach(Unchecked.consumer(statement -> {

        String tableName = getRdbmsResolver().rdbmsTable(statement.getInstance().getType()).getSqlName();
        ID identifier = statement.getInstance().getIdentifier();

        String sql = "SELECT count(1) FROM " + tableName + " WHERE ID = :" + getIdentifierProvider().getName();

        if (log.isDebugEnabled()) {
            log.debug("EntityExistsCheck: " + AsmUtils.getClassifierFQName(statement.getInstance().getType()) +
                    " " + tableName + " ID: " + identifier + " SQL: " + sql);
        }

        SqlParameterSource namedParameters = new MapSqlParameterSource()
                .addValue(getIdentifierProvider().getName(),
                        getCoercer().coerce(statement.getInstance().getIdentifier(),
                                getRdbmsParameterMapper().getIdClassName()), getRdbmsParameterMapper().getIdSqlType());

        int count = jdbcTemplate.queryForObject(sql, namedParameters, Integer.class);

        if (count == 0) {
            final Map<String, Object> details = new LinkedHashMap<>();
            details.put(getIdentifierProvider().getName(), identifier);
            details.put(ENTITY_TYPE_MAP_KEY, AsmUtils.getClassifierFQName(statement.getInstance().getType()));
            throw new ValidationException("Instance not found", Collections.singleton(ValidationResult.builder()
                    .code("ENTITY_NOT_FOUND")
                    .level(ValidationResult.Level.ERROR)
                    .details(details)
                    .build()));
        } else {
            checkState(count == 1,
                    String.format("Entity %s (%s) with identifier %s exists multiple times.",
                            AsmUtils.getClassifierFQName(statement.getInstance().getType()),
                            tableName,
                            identifier));
        }

        }));
    }
}
