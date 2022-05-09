package hu.blackbelt.judo.runtime.core.dao.rdbms.executors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.rdbms.RdbmsField;
import hu.blackbelt.judo.meta.rdbms.RdbmsTable;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.core.statements.ReferenceStatement;
import hu.blackbelt.judo.runtime.core.dao.core.statements.RemoveReferenceStatement;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsReference;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
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
import static java.util.stream.Stream.concat;

/**
 * Executing {@link RemoveReferenceStatement} statements.
 * It analyzes where the foreign key are presented and making update in the owner table. If it is a join table
 * record is deleted.
 *
 * @param <ID>
 */
@Slf4j(topic = "dao-rdbms")
class RemoveReferenceStatementExecutor<ID> extends StatementExecutor<ID> {

    @Builder
    public RemoveReferenceStatementExecutor(AsmModel asmModel, RdbmsModel rdbmsModel,
                                            TransformationTraceService transformationTraceService, RdbmsParameterMapper rdbmsParameterMapper,
                                            Coercer coercer, IdentifierProvider<ID> identifierProvider, Dialect dialect) {
        super(asmModel, rdbmsModel, transformationTraceService, rdbmsParameterMapper, coercer, identifierProvider, dialect);
    }

    /**
     * Collecting {@link RemoveReferenceStatement} statements from statement list and execute it.
     * @param jdbcTemplate
     * @param statements
     * @throws SQLException
     */
    public void executeRemoveReferenceStatements(NamedParameterJdbcTemplate jdbcTemplate,
                                                 Collection<ReferenceStatement<ID>> statements) throws SQLException {

        RdbmsResolver rdbms = new RdbmsResolver(asmModel, transformationTraceService);

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
                                        new MapSqlParameterSource().addValue(identifierProvider.getName(), coercer.coerce(identifier.get(), rdbmsParameterMapper.getIdClassName()), rdbmsParameterMapper.getIdSqlType());
                                updateStatementNamedParameters.addValue(getReferenceFQName(reference), coercer.coerce(referenceIdentifier.get(), rdbmsParameterMapper.getIdClassName()), rdbmsParameterMapper.getIdSqlType());

                                String tableName = rdbms.rdbmsTable(entity.get()).getSqlName();

                                String sql =
                                        "UPDATE " + tableName + " SET " + rdbms.rdbmsField(reference).getSqlName() + " = NULL"
                                                + " WHERE " + ID_COLUMN_NAME + " = :" + identifierProvider.getName();

                                log.debug("Remove references: " + getClassifierFQName(entityForCurrentStatement) + " " + tableName +
                                        " ID: " + identifier.get() +
                                        " REF ID: " + referenceIdentifier.get() +
                                        " SQL: " + sql +
                                        " Params: " + ImmutableMap.copyOf(updateStatementNamedParameters.getValues()).toString());

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
                                r.getReference().equals(((ReferenceStatement) r.getStatement()).getReference())
                )
                .forEach(r -> {

                    RdbmsTable joinTable = rdbms.rdbmsJunctionTable(r.getReference());
                    RdbmsField aFk = rdbms.rdbmsJunctionOppositeField(r.getReference());
                    RdbmsField bFk = rdbms.rdbmsJunctionField(r.getReference());
                    ID aId = r.getIdentifier();
                    ID bId = r.getOppositeIdentifier();

                    SqlParameterSource jointPairNamedParameters = new MapSqlParameterSource()
                            .addValue("id", coercer.coerce(UUID.randomUUID(), rdbmsParameterMapper.getIdClassName()), rdbmsParameterMapper.getIdSqlType())
                            .addValue("aId", coercer.coerce(aId, rdbmsParameterMapper.getIdClassName()), rdbmsParameterMapper.getIdSqlType())
                            .addValue("bId", coercer.coerce(bId, rdbmsParameterMapper.getIdClassName()), rdbmsParameterMapper.getIdSqlType());

                    // Check existence of the given ID pair.
                    String exitenceCheckSql = "SELECT count(1) FROM " + joinTable.getSqlName() + " WHERE " +
                            aFk.getSqlName() + " = :aId AND " +
                            bFk.getSqlName() + " = :bId";

                    log.debug("JoinTableExistsCheck: " + AsmUtils.getClassifierFQName(
                            r.getStatement().getInstance().getType()) +
                            " " + joinTable.getSqlName() + " A ID: " + aId +
                            " B ID: " + bId  + " SQL: " + exitenceCheckSql);

                    int count = jdbcTemplate.queryForObject(exitenceCheckSql, jointPairNamedParameters, Integer.class);

                    checkState(count == 1,
                            String.format("The given reference %s (%s) with identifier %s and identifier %s does not exists.",
                                    r.getStatement().getInstance().getType(),
                                    joinTable.getSqlName(),
                                    aId,
                                    bId));


                    // Delete reference
                    String deleteJoinTableSql = "DELETE FROM " + joinTable.getSqlName() + " WHERE " +
                            aFk.getSqlName() + " = :aId AND " + bFk.getSqlName() + " = :bId";

                    log.debug("Remove reference: " + AsmUtils.getClassifierFQName(
                            r.getStatement().getInstance().getType()) +
                            " " + joinTable.getSqlName() + " A ID: " + aId +
                            " B ID: " + bId  + " SQL: " + exitenceCheckSql);

                    count = jdbcTemplate.update(deleteJoinTableSql, jointPairNamedParameters);
                    checkState(count == 1, "There is illegal state, no records updated on delete reference");
                });
    }
}
