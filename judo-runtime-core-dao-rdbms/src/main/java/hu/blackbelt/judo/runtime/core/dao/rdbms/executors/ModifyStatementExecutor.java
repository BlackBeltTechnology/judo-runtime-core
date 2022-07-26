package hu.blackbelt.judo.runtime.core.dao.rdbms.executors;

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

import java.sql.SQLException;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Executes banch of statements and making proper execution order for the given statements.
 * @param <ID>
 */
public class ModifyStatementExecutor<ID> extends StatementExecutor<ID> {

    @Builder
    public ModifyStatementExecutor(
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
     * Executing all given statements. There is precedence is used for statements.
     *
     * @param jdbcTemplate
     * @param statements
     * @throws SQLException
     */
    public void executeStatements(NamedParameterJdbcTemplate jdbcTemplate,
                                  Collection<Statement<ID>> statements) throws SQLException {

        EntityExistsValidationStatementExecutor<ID> entityExistsValidationStatementExecutor =
                EntityExistsValidationStatementExecutor.<ID>builder()
                        .asmModel(getAsmModel())
                        .rdbmsModel(getRdbmsModel())
                        .rdbmsResolver(getRdbmsResolver())
                        .transformationTraceService(getTransformationTraceService())
                        .rdbmsParameterMapper(getRdbmsParameterMapper())
                        .rdbmsResolver(getRdbmsResolver())
                        .coercer(getCoercer())
                        .identifierProvider(getIdentifierProvider())
                        .build();


        InsertStatementExecutor<ID> insertStatementExecutor = InsertStatementExecutor.<ID>builder()
                .asmModel(getAsmModel())
                .rdbmsModel(getRdbmsModel())
                .rdbmsResolver(getRdbmsResolver())
                .transformationTraceService(getTransformationTraceService())
                .rdbmsParameterMapper(getRdbmsParameterMapper())
                .rdbmsResolver(getRdbmsResolver())
                .coercer(getCoercer())
                .identifierProvider(getIdentifierProvider())
                .build();

        UpdateStatementExecutor<ID> updateStatementExecutor = UpdateStatementExecutor.<ID>builder()
                .asmModel(getAsmModel())
                .rdbmsModel(getRdbmsModel())
                .rdbmsResolver(getRdbmsResolver())
                .transformationTraceService(getTransformationTraceService())
                .rdbmsParameterMapper(getRdbmsParameterMapper())
                .rdbmsResolver(getRdbmsResolver())
                .coercer(getCoercer())
                .identifierProvider(getIdentifierProvider())
                .build();

        CheckUniqueAttributeStatementExecutor<ID> checkUniqueAttributeStatementExecutor = CheckUniqueAttributeStatementExecutor.<ID>builder()
                .asmModel(getAsmModel())
                .rdbmsModel(getRdbmsModel())
                .rdbmsResolver(getRdbmsResolver())
                .transformationTraceService(getTransformationTraceService())
                .rdbmsParameterMapper(getRdbmsParameterMapper())
                .rdbmsResolver(getRdbmsResolver())
                .coercer(getCoercer())
                .identifierProvider(getIdentifierProvider())
                .build();

        UpdateReferenceExecutor<ID> updateReferenceExecutor = UpdateReferenceExecutor.<ID>builder()
                .asmModel(getAsmModel())
                .rdbmsModel(getRdbmsModel())
                .rdbmsResolver(getRdbmsResolver())
                .transformationTraceService(getTransformationTraceService())
                .rdbmsParameterMapper(getRdbmsParameterMapper())
                .rdbmsResolver(getRdbmsResolver())
                .coercer(getCoercer())
                .identifierProvider(getIdentifierProvider())
                .build();

        DeleteStatementExecutor<ID> deleteStatementExecutor = DeleteStatementExecutor.<ID>builder()
                .asmModel(getAsmModel())
                .rdbmsModel(getRdbmsModel())
                .rdbmsResolver(getRdbmsResolver())
                .transformationTraceService(getTransformationTraceService())
                .rdbmsParameterMapper(getRdbmsParameterMapper())
                .rdbmsResolver(getRdbmsResolver())
                .coercer(getCoercer())
                .identifierProvider(getIdentifierProvider())
                .build();

        AddReferenceStatementExecutor<ID> addReferenceStatementExecutor = AddReferenceStatementExecutor.<ID>builder()
                .asmModel(getAsmModel())
                .rdbmsModel(getRdbmsModel())
                .rdbmsResolver(getRdbmsResolver())
                .transformationTraceService(getTransformationTraceService())
                .rdbmsParameterMapper(getRdbmsParameterMapper())
                .rdbmsResolver(getRdbmsResolver())
                .coercer(getCoercer())
                .identifierProvider(getIdentifierProvider())
                .build();

        RemoveReferenceStatementExecutor<ID> removeReferenceStatementExecutor = RemoveReferenceStatementExecutor.<ID>builder()
                .asmModel(getAsmModel())
                .rdbmsModel(getRdbmsModel())
                .rdbmsResolver(getRdbmsResolver())
                .transformationTraceService(getTransformationTraceService())
                .rdbmsParameterMapper(getRdbmsParameterMapper())
                .rdbmsResolver(getRdbmsResolver())
                .coercer(getCoercer())
                .identifierProvider(getIdentifierProvider())
                .build();

        AddRemoveReferenceStatementConsistencyCheckExecutor<ID> addRemoveReferenceStatementConsistencyCheckExecutor = AddRemoveReferenceStatementConsistencyCheckExecutor.<ID>builder()
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
                        .map(o -> (InstanceExistsValidationStatement<ID>) o)
                        .collect(Collectors.toList())
        );

        // Check remove references
        addRemoveReferenceStatementConsistencyCheckExecutor.checkRemoveReferenceStatements(
                jdbcTemplate,
                statements.stream()
                        .filter(RemoveReferenceStatement.class :: isInstance)
                        .map(o -> (RemoveReferenceStatement<ID>) o)
                        .collect(Collectors.toList()),
                statements.stream()
                        .filter(DeleteStatement.class :: isInstance)
                        .map(o -> ((DeleteStatement<ID>) o).getInstance().getIdentifier())
                        .collect(Collectors.toList())
        );

        // Check remove references
        addRemoveReferenceStatementConsistencyCheckExecutor.checkAddReferenceStatements(
                jdbcTemplate,
                statements.stream()
                        .filter(AddReferenceStatement.class :: isInstance)
                        .map(o -> (AddReferenceStatement<ID>) o)
                        .collect(Collectors.toList())
        );

        // Remove references
        removeReferenceStatementExecutor.executeRemoveReferenceStatements(
                jdbcTemplate,
                statements.stream()
                        .filter(RemoveReferenceStatement.class :: isInstance)
                        .map(o -> (RemoveReferenceStatement<ID>) o)
                        .collect(Collectors.toList())
        );

        // Remove deleted entities
        deleteStatementExecutor.executeDeleteStatements(
                jdbcTemplate,
                statements.stream()
                        .filter(DeleteStatement.class :: isInstance)
                        .map(o -> (DeleteStatement<ID>) o)
                        .collect(Collectors.toList()),

                statements.stream()
                        .filter(RemoveReferenceStatement.class :: isInstance)
                        .map(o -> (RemoveReferenceStatement<ID>) o)
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
                        .map(o -> (InsertStatement<ID>) o)
                        .collect(Collectors.toList()),

                statements.stream()
                        .filter(AddReferenceStatement.class :: isInstance)
                        .map(o -> (AddReferenceStatement<ID>) o)
                        .collect(Collectors.toList()));

        // Update existing entities
        updateStatementExecutor.executeUpdateStatements(
                jdbcTemplate,
                statements.stream()
                        .filter(UpdateStatement.class :: isInstance)
                        .map(o -> (UpdateStatement<ID>) o)
                        .collect(Collectors.toList())
                );

        // Those addReferences which has removeReferences too - which means update
        Collection<AddReferenceStatement<ID>> addReferenceStatementsExistsInRemoveReferenceStatements =
                statements.stream()
                        .filter(AddReferenceStatement.class :: isInstance)
                        .map(o -> (AddReferenceStatement<ID>) o)
                        .filter(r -> statements.stream()
                                .filter(RemoveReferenceStatement.class :: isInstance)
                                .map(o -> (RemoveReferenceStatement<ID>) o).filter(
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
                        .map(o -> (AddReferenceStatement<ID>) o)
//                        .filter(o -> !addReferenceStatementsExistsInRemoveReferenceStatements.contains(o))
                        .collect(Collectors.toList())
        );

        // TODO: Check boundary contraints on relations
    }

}
