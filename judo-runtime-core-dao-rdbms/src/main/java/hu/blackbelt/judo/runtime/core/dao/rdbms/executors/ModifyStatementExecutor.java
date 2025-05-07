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

import com.google.common.collect.Streams;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.core.statements.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.NonNull;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Executes banch of statements and making proper execution order for the given statements.
 */
public class ModifyStatementExecutor extends StatementExecutor {

    @Builder
    public ModifyStatementExecutor(
            @NonNull AsmModel asmModel,
            @NonNull RdbmsModel rdbmsModel,
            @NonNull TransformationTraceService transformationTraceService,
            @NonNull RdbmsParameterMapper rdbmsParameterMapper,
            @NonNull RdbmsResolver rdbmsResolver,
            @NonNull Coercer coercer,
            @NonNull IdentifierProvider identifierProvider) {
        super(asmModel, rdbmsModel, transformationTraceService, rdbmsParameterMapper, rdbmsResolver, coercer, identifierProvider);
    }

    /**
     * Executing all given statements. There is precedence is used for statements.
     *
     * @param jdbcTemplate
     * @param statements
     * @throws SQLException
     */
    public void executeStatements(NamedParameterJdbcTemplate jdbcTemplate,
                                  Collection<Statement> statements) throws SQLException {

        EntityExistsValidationStatementExecutor entityExistsValidationStatementExecutor =
                EntityExistsValidationStatementExecutor.builder()
                        .asmModel(getAsmModel())
                        .rdbmsModel(getRdbmsModel())
                        .rdbmsResolver(getRdbmsResolver())
                        .transformationTraceService(getTransformationTraceService())
                        .rdbmsParameterMapper(getRdbmsParameterMapper())
                        .rdbmsResolver(getRdbmsResolver())
                        .coercer(getCoercer())
                        .identifierProvider(getIdentifierProvider())
                        .build();


        InsertStatementExecutor insertStatementExecutor = InsertStatementExecutor.builder()
                .asmModel(getAsmModel())
                .rdbmsModel(getRdbmsModel())
                .rdbmsResolver(getRdbmsResolver())
                .transformationTraceService(getTransformationTraceService())
                .rdbmsParameterMapper(getRdbmsParameterMapper())
                .rdbmsResolver(getRdbmsResolver())
                .coercer(getCoercer())
                .identifierProvider(getIdentifierProvider())
                .build();

        UpdateStatementExecutor updateStatementExecutor = UpdateStatementExecutor.<Serializable>builder()
                .asmModel(getAsmModel())
                .rdbmsModel(getRdbmsModel())
                .rdbmsResolver(getRdbmsResolver())
                .transformationTraceService(getTransformationTraceService())
                .rdbmsParameterMapper(getRdbmsParameterMapper())
                .rdbmsResolver(getRdbmsResolver())
                .coercer(getCoercer())
                .identifierProvider(getIdentifierProvider())
                .build();

        CheckUniqueAttributeStatementExecutor checkUniqueAttributeStatementExecutor = CheckUniqueAttributeStatementExecutor.builder()
                .asmModel(getAsmModel())
                .rdbmsModel(getRdbmsModel())
                .rdbmsResolver(getRdbmsResolver())
                .transformationTraceService(getTransformationTraceService())
                .rdbmsParameterMapper(getRdbmsParameterMapper())
                .rdbmsResolver(getRdbmsResolver())
                .coercer(getCoercer())
                .identifierProvider(getIdentifierProvider())
                .build();

        UpdateReferenceExecutor updateReferenceExecutor = UpdateReferenceExecutor.builder()
                .asmModel(getAsmModel())
                .rdbmsModel(getRdbmsModel())
                .rdbmsResolver(getRdbmsResolver())
                .transformationTraceService(getTransformationTraceService())
                .rdbmsParameterMapper(getRdbmsParameterMapper())
                .rdbmsResolver(getRdbmsResolver())
                .coercer(getCoercer())
                .identifierProvider(getIdentifierProvider())
                .build();

        DeleteStatementExecutor deleteStatementExecutor = DeleteStatementExecutor.builder()
                .asmModel(getAsmModel())
                .rdbmsModel(getRdbmsModel())
                .rdbmsResolver(getRdbmsResolver())
                .transformationTraceService(getTransformationTraceService())
                .rdbmsParameterMapper(getRdbmsParameterMapper())
                .rdbmsResolver(getRdbmsResolver())
                .coercer(getCoercer())
                .identifierProvider(getIdentifierProvider())
                .build();

        AddReferenceStatementExecutor addReferenceStatementExecutor = AddReferenceStatementExecutor.builder()
                .asmModel(getAsmModel())
                .rdbmsModel(getRdbmsModel())
                .rdbmsResolver(getRdbmsResolver())
                .transformationTraceService(getTransformationTraceService())
                .rdbmsParameterMapper(getRdbmsParameterMapper())
                .rdbmsResolver(getRdbmsResolver())
                .coercer(getCoercer())
                .identifierProvider(getIdentifierProvider())
                .build();

        RemoveReferenceStatementExecutor removeReferenceStatementExecutor = RemoveReferenceStatementExecutor.builder()
                .asmModel(getAsmModel())
                .rdbmsModel(getRdbmsModel())
                .rdbmsResolver(getRdbmsResolver())
                .transformationTraceService(getTransformationTraceService())
                .rdbmsParameterMapper(getRdbmsParameterMapper())
                .rdbmsResolver(getRdbmsResolver())
                .coercer(getCoercer())
                .identifierProvider(getIdentifierProvider())
                .build();

        AddRemoveReferenceStatementConsistencyCheckExecutor addRemoveReferenceStatementConsistencyCheckExecutor = AddRemoveReferenceStatementConsistencyCheckExecutor.builder()
                .asmModel(getAsmModel())
                .rdbmsModel(getRdbmsModel())
                .rdbmsResolver(getRdbmsResolver())
                .transformationTraceService(getTransformationTraceService())
                .rdbmsParameterMapper(getRdbmsParameterMapper())
                .rdbmsResolver(getRdbmsResolver())
                .coercer(getCoercer())
                .identifierProvider(getIdentifierProvider())
                .build();

        // Check existence
        entityExistsValidationStatementExecutor.executeEntityExistsValidationStatements(
                jdbcTemplate,
                statements.stream()
                        .filter(InstanceExistsValidationStatement.class :: isInstance)
                        .map(o -> (InstanceExistsValidationStatement) o)
                        .collect(Collectors.toList())
        );

        // Check remove references
        addRemoveReferenceStatementConsistencyCheckExecutor.checkRemoveReferenceStatements(
                jdbcTemplate,
                statements.stream()
                        .filter(RemoveReferenceStatement.class :: isInstance)
                        .map(o -> (RemoveReferenceStatement) o)
                        .collect(Collectors.toList()),
                statements.stream()
                        .filter(DeleteStatement.class :: isInstance)
                        .map(o -> ((DeleteStatement) o).getInstance().getIdentifier())
                        .collect(Collectors.toList())
        );

        // Check remove references
        addRemoveReferenceStatementConsistencyCheckExecutor.checkAddReferenceStatements(
                jdbcTemplate,
                statements.stream()
                        .filter(AddReferenceStatement.class :: isInstance)
                        .map(o -> (AddReferenceStatement) o)
                        .collect(Collectors.toList())
        );

        // Remove references
        removeReferenceStatementExecutor.executeRemoveReferenceStatements(
                jdbcTemplate,
                statements.stream()
                        .filter(RemoveReferenceStatement.class :: isInstance)
                        .map(o -> (RemoveReferenceStatement) o)
                        .collect(Collectors.toList())
        );

        // Remove deleted entities
        deleteStatementExecutor.executeDeleteStatements(
                jdbcTemplate,
                statements.stream()
                        .filter(DeleteStatement.class :: isInstance)
                        .map(o -> (DeleteStatement) o)
                        .collect(Collectors.toList()),

                statements.stream()
                        .filter(RemoveReferenceStatement.class :: isInstance)
                        .map(o -> (RemoveReferenceStatement) o)
                        .collect(Collectors.toList())
        );

        // Collect insert / update records
        checkUniqueAttributeStatementExecutor.executeUniqueAttributeStatements(
                jdbcTemplate,
                Streams.concat(
                    statements.stream()
                            .filter(InsertStatement.class :: isInstance)
                            .map(CheckUniqueAttributeStatement::fromStatement),
                    statements.stream()
                            .filter(UpdateStatement.class :: isInstance)
                            .map(CheckUniqueAttributeStatement::fromStatement))
                    .collect(Collectors.toList())
        );

        // Insert new entities
        insertStatementExecutor.executeInsertStatements(
                jdbcTemplate,
                statements.stream()
                        .filter(InsertStatement.class :: isInstance)
                        .map(o -> (InsertStatement) o)
                        .collect(Collectors.toList()),

                statements.stream()
                        .filter(AddReferenceStatement.class :: isInstance)
                        .map(o -> (AddReferenceStatement) o)
                        .collect(Collectors.toList()));

        // Update existing entities
        updateStatementExecutor.executeUpdateStatements(
                jdbcTemplate,
                statements.stream()
                        .filter(UpdateStatement.class :: isInstance)
                        .map(o -> (UpdateStatement) o)
                        .collect(Collectors.toList())
                );

        // Those addReferences which has removeReferences too - which means update
        Collection<AddReferenceStatement> addReferenceStatementsExistsInRemoveReferenceStatements =
                statements.stream()
                        .filter(AddReferenceStatement.class :: isInstance)
                        .map(o -> (AddReferenceStatement) o)
                        .filter(r -> statements.stream()
                                .filter(RemoveReferenceStatement.class :: isInstance)
                                .map(o -> (RemoveReferenceStatement) o).filter(
                                r2 -> r2.getIdentifier().equals(r.getIdentifier()) && r2.getReference().equals(r.getReference())
                        ).findFirst().isPresent())
                        .collect(Collectors.toSet());

        // Update existing references
        updateReferenceExecutor.executeReferenceUpdateStatements(
                jdbcTemplate,
                addReferenceStatementsExistsInRemoveReferenceStatements
        );

        // TODO: Maybe some update statement is duplicated
        // Add references
        addReferenceStatementExecutor.executeAddReferenceStatements(
                jdbcTemplate,
                statements.stream()
                        .filter(AddReferenceStatement.class :: isInstance)
                        .map(o -> (AddReferenceStatement) o)
//                        .filter(o -> !addReferenceStatementsExistsInRemoveReferenceStatements.contains(o))
                        .collect(Collectors.toList())
        );

        // TODO: Check boundary contraints on relations
    }

}
