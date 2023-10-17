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
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.dao.core.statements.CheckUniqueAttributeStatement;
import hu.blackbelt.judo.runtime.core.dao.core.values.AttributeValue;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.getClassifierFQName;
import static hu.blackbelt.judo.meta.asm.runtime.AsmUtils.isEntityType;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;
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
     * It checks all Identifier type fields uniqueness. Checking is done by one statement per table.
     *
     * @param jdbcTemplate
     * @param checkUniqueAttributeStatements
     */
    public void executeUniqueAttributeStatements(NamedParameterJdbcTemplate jdbcTemplate,
                                                 Collection<CheckUniqueAttributeStatement<ID>> checkUniqueAttributeStatements) {

        // Search for duplication
        Map<ID, CheckUniqueAttributeStatement<ID>> checkUniqueAttributeStatementsCompacted = new HashMap<ID, CheckUniqueAttributeStatement<ID>>();

        checkUniqueAttributeStatements.forEach(item -> {
            ID identifier = item.getInstance().getIdentifier();
            if (!checkUniqueAttributeStatementsCompacted.containsKey(identifier)) {
                checkUniqueAttributeStatementsCompacted.put(identifier, item);
            } else {
                CheckUniqueAttributeStatement previous = checkUniqueAttributeStatementsCompacted.get(identifier);
                CheckUniqueAttributeStatement aggregatedItem = previous.mergeAttributes(item);
                checkUniqueAttributeStatementsCompacted.put(identifier, aggregatedItem);
            }
        });

        // Collecting same attributes and values with number of occurrences, and filters out where
        // the occurrence more than 1
        Map<EAttribute, Map<Object, Long>> attributesViolatesValueUniqueness = checkUniqueAttributeStatementsCompacted.values().stream()
                .flatMap(e -> e.getInstance().getAttributes().stream()
                        .filter(a -> a.getValue() != null)
                        .filter(a -> AsmUtils.isIdentifier(a.getAttribute()))
                        .collect(toMap(
                                a -> a.getAttribute(),
                                a -> a.getValue()))
                        .entrySet().stream())
                .collect(groupingBy(
                        e -> e.getKey(),
                        mapping(e -> e.getValue(),
                                groupingBy(identity(), counting()))))
                .entrySet().stream()
                .filter(e -> e.getValue().entrySet().stream()
                                .filter(e2 -> e2.getValue() > 1)
                                .count() > 0)
                .collect(toMap(e -> e.getKey(), e -> e.getValue()));

        if (attributesViolatesValueUniqueness.size() != 0) {
            throw new IllegalStateException("Identifier uniqueness violation(s): " + attributesViolatesValueUniqueness.entrySet()
                    .stream()
                    .map(e -> e.getKey().getName() + "=" + e.getValue().keySet().stream().findFirst().get())
                    .collect(Collectors.joining(",")));
        }


        checkUniqueAttributeStatementsCompacted.values().forEach(consumer(checkUniqueAttributeStatement -> {

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

                                Map<EAttribute, Object> attributeMapForCurrentStatement = attributeMap.entrySet().stream()
                                        .filter(e -> e.getKey().eContainer().equals(entityForCurrentStatement))
                                        .filter(e -> AsmUtils.isIdentifier(e.getKey()))
                                        .collect(HashMap::new, (m,v) -> m.put(
                                                v.getKey(),
                                                v.getValue()),
                                                HashMap::putAll);

                                if (attributeMapForCurrentStatement.size() > 0) {

                                    MapSqlParameterSource checkUniqueIdentifiersStatementNamedParameters = new MapSqlParameterSource();

                                    checkUniqueIdentifiersStatementNamedParameters.addValue(getIdentifierProvider().getName(), getCoercer().coerce(identifier, getRdbmsParameterMapper().getIdClassName()), getRdbmsParameterMapper().getIdSqlType());

                                    getRdbmsResolver().logAttributeParameters(attributeMapForCurrentStatement);

                                    getRdbmsParameterMapper().mapAttributeParameters(checkUniqueIdentifiersStatementNamedParameters, attributeMapForCurrentStatement);

                                    Map<String, String> fields =
                                            attributeMapForCurrentStatement.keySet().stream()
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

                                    if (log.isDebugEnabled()) {
                                        log.debug("Check unique identifier: " + getClassifierFQName(entityForCurrentStatement) + " " + tableName +
                                                " ID: " + identifier +
                                                " SQL: " + sql +
                                                " Params: " + checkUniqueIdentifiersStatementNamedParameters.getValues());
                                    }

                                    List<Map<String, Object>> violations = jdbcTemplate.queryForList(sql, checkUniqueIdentifiersStatementNamedParameters);

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
