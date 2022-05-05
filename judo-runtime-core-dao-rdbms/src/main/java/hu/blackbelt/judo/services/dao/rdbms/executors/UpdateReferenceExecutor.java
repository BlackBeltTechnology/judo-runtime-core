package hu.blackbelt.judo.services.dao.rdbms.executors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.services.dao.core.statements.AddReferenceStatement;
import hu.blackbelt.judo.services.dao.core.statements.RemoveReferenceStatement;
import hu.blackbelt.judo.services.dao.core.statements.UpdateStatement;
import hu.blackbelt.judo.services.dao.rdbms.Dialect;
import hu.blackbelt.judo.services.dao.rdbms.RdbmsReference;
import hu.blackbelt.judo.services.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EReference;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.Collection;
import java.util.HashMap;
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

    public UpdateReferenceExecutor(AsmModel asmModel, RdbmsModel rdbmsModel,
                                   TransformationTraceService transformationTraceService, Coercer coercer,
                                   IdentifierProvider<ID> identifierProvider, Dialect dialect) {
        super(asmModel, rdbmsModel, transformationTraceService, coercer, identifierProvider, dialect);
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

        RdbmsResolver rdbms = new RdbmsResolver(asmModel, transformationTraceService);

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
                                        .addValue(identifierProvider.getName(), coercer.coerce(identifier, parameterMapper.getIdClassName()), parameterMapper.getIdSqlType());

                                Map<EReference, Object> updateReferenceMapForCurrentStatement = updateReferenceMap.entrySet().stream()
                                        .filter(e -> (e.getKey().getReference().eContainer().equals(entityForCurrentStatement)) ||
                                                (e.getKey().getReference().getEReferenceType().equals(entityForCurrentStatement)))
                                        .collect(toMap(e -> e.getKey().getReference(), Map.Entry::getValue));

                                rdbms.logReferenceParameters(updateReferenceMapForCurrentStatement);
                                parameterMapper.mapReferenceParameters(updateStatementNamedParameters, updateReferenceMapForCurrentStatement);

                                Map<String, String> fields =
                                                updateReferenceMapForCurrentStatement.keySet().stream()
                                                        .collect(toMap(
                                                                e -> e.getName(),
                                                                e -> rdbms.rdbmsField(e).getSqlName()))
                                                        .entrySet().stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

                                if (fields.size() > 0) {

                                    String tableName = rdbms.rdbmsTable(entityForCurrentStatement).getSqlName();

                                    String sql = "UPDATE  " + tableName + " SET " +
                                            fields.entrySet().stream().map(e -> e.getValue() + " = :" + e.getKey()).collect(Collectors.joining(", ")) +
                                            " WHERE " + ID_COLUMN_NAME + " = :" + identifierProvider.getName();

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
