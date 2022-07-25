package hu.blackbelt.judo.runtime.core.dao.rdbms.executors;

import com.google.common.collect.ImmutableList;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.core.statements.CheckUniqueAttributeStatement;
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

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.getClassifierFQName;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.isEntityType;
import static java.util.stream.Collectors.toMap;
import static org.jooq.lambda.Unchecked.consumer;

/**
 * Executing {@link CheckUniqueAttributeStatement}s.
 * @param <ID>
 */
@Slf4j(topic = "dao-rdbms")
class CheckUniqueAttributeStatementExecutor<ID> extends StatementExecutor<ID> {

    @Builder
    public CheckUniqueAttributeStatementExecutor(
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
     * It executes {@link CheckUniqueAttributeStatement} instances
     *
     * It checks all Identifier type fields uniqueness. It checking it in one statement per table.
     *
     * @param jdbcTemplate
     * @param checkUniqueAttributeStatements
     */
    public void executeUpdateStatements(NamedParameterJdbcTemplate jdbcTemplate,
                                        Collection<CheckUniqueAttributeStatement<ID>> checkUniqueAttributeStatements) {

        checkUniqueAttributeStatements.forEach(consumer(checkUniqueAttributeStatement -> {

                    EClass entity = checkUniqueAttributeStatement.getInstance().getType();
                    ID identifier = checkUniqueAttributeStatement.getInstance().getIdentifier();

                    // Collect columns
                    Map<EAttribute, Object> attributeMap = checkUniqueAttributeStatement.getInstance().getAttributes().stream()
                            .collect(HashMap::new, (m,v) -> m.put(
                                    v.getAttribute(),
                                    v.getValue()),
                                    HashMap::putAll);

                    // Collecting all tables on the inheritance chain which required to insert.
                    Stream.concat(
                            ImmutableList.of(entity).stream(),
                            entity.getEAllSuperTypes().stream()).filter(t -> isEntityType(t))
                            .forEach(entityForCurrentStatement -> {

                                Map<EAttribute, Object> attributeMapforCurrentStatement = attributeMap.entrySet().stream()
                                        .filter(e -> e.getKey().eContainer().equals(entityForCurrentStatement))
                                        .filter(e -> AsmUtils.isIdentifier(e.getKey()))
                                        .collect(HashMap::new, (m,v) -> m.put(
                                                v.getKey(),
                                                v.getValue()),
                                                HashMap::putAll);

                                if (attributeMapforCurrentStatement.size() > 0) {

                                    MapSqlParameterSource chekUniqueIdentifiersStatementNamedParameters = new MapSqlParameterSource();

                                    chekUniqueIdentifiersStatementNamedParameters.addValue(getIdentifierProvider().getName(), getCoercer().coerce(identifier, getRdbmsParameterMapper().getIdClassName()), getRdbmsParameterMapper().getIdSqlType());

                                    getRdbmsResolver().logAttributeParameters(attributeMapforCurrentStatement);

                                    getRdbmsParameterMapper().mapAttributeParameters(chekUniqueIdentifiersStatementNamedParameters, attributeMapforCurrentStatement);

                                    Map<String, String> fields =
                                            attributeMapforCurrentStatement.keySet().stream()
                                                    .collect(toMap(
                                                            e -> e.getName(),
                                                            e -> getRdbmsResolver().rdbmsField(e).getSqlName()))
                                                    .entrySet().stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

                                    String tableName = getRdbmsResolver().rdbmsTable(entityForCurrentStatement).getSqlName();

                                    String fieldsMatching = fields.entrySet().stream()
                                            .map(e -> e.getValue() + " = :" + e.getKey() + " AS \"" + e.getKey() + "\"")
                                            .collect(Collectors.joining(","));

                                    String where  = " WHERE " + ID_COLUMN_NAME + " <> :" + getIdentifierProvider().getName() + " AND (" +
                                            fields.entrySet().stream()
                                                    .map(e -> e.getValue() + " = :" + e.getKey())
                                                    .collect(Collectors.joining(" OR ")) + ")";

                                    String sql = "SELECT " + fieldsMatching + " FROM " + tableName + where;

                                    log.debug("Check unique identifier: " + getClassifierFQName(entityForCurrentStatement) + " " + tableName +
                                            " ID: " + identifier +
                                            " SQL: " + sql +
                                            " Params: " + chekUniqueIdentifiersStatementNamedParameters.getValues());

                                    List<Map<String, Object>> violations = jdbcTemplate.queryForList(sql, chekUniqueIdentifiersStatementNamedParameters);

                                    if (violations.size() != 0) {
                                        throw new IllegalStateException("Identifier uniqueness violation(s): " + violations.get(0).entrySet()
                                                .stream()
                                                .filter(v -> v.getValue() != null)
                                                .map(e -> e.getKey() + "=" + attributeMap.entrySet().stream().filter(a -> a.getKey().getName().equals(e.getKey())).findFirst().get().getValue())
                                                .collect(Collectors.joining(",")));
                                    }
                                }
                            });
                }));
    }
}
