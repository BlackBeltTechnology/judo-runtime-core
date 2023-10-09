package hu.blackbelt.judo.runtime.core.dao.rdbms.query;

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
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.rdbmsRules.Rule;
import hu.blackbelt.judo.meta.rdbmsRules.Rules;
import hu.blackbelt.judo.runtime.core.dao.rdbms.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.SelectStatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.MapperFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.RdbmsMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.join.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil;
import hu.blackbelt.mapper.api.Coercer;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.*;
import org.eclipse.emf.ecore.*;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class RdbmsBuilder<ID> {

    @Getter
    private final RdbmsResolver rdbmsResolver;

    @Getter
    private final RdbmsParameterMapper<ID> parameterMapper;

    @Getter
    private final IdentifierProvider<ID> identifierProvider;

    @Getter
    private final Coercer coercer;

    @Getter
    private final AtomicInteger constantCounter = new AtomicInteger(0);

    @Getter
    private final AncestorNameFactory ancestorNameFactory;

    @Getter
    private final DescendantNameFactory descendantNameFactory;

    @Getter
    private final  VariableResolver variableResolver;

    @Getter
    private final  RdbmsModel rdbmsModel;

    @Getter
    private final AsmUtils asmUtils;

    private final Map<Class<?>, RdbmsMapper<?>> mappers;

    private final Rules rules;

    @Getter
    private final Dialect dialect;

    private final ThreadLocal<Map<String, Collection<? extends RdbmsField>>> CONSTANT_FIELDS = new ThreadLocal<>();

    @Builder
    public RdbmsBuilder(
            @NonNull RdbmsResolver rdbmsResolver,
            @NonNull RdbmsParameterMapper<ID> parameterMapper,
            @NonNull IdentifierProvider<ID> identifierProvider,
            @NonNull Coercer coercer,
            @NonNull AncestorNameFactory ancestorNameFactory,
            @NonNull DescendantNameFactory descendantNameFactory,
            @NonNull VariableResolver variableResolver,
            @NonNull RdbmsModel rdbmsModel,
            @NonNull AsmUtils asmUtils,
            @NonNull MapperFactory<ID> mapperFactory,
            @NonNull Dialect dialect) {
        this.rdbmsResolver = rdbmsResolver;
        this.parameterMapper = parameterMapper;
        this.identifierProvider = identifierProvider;
        this.coercer = coercer;
        this.ancestorNameFactory = ancestorNameFactory;
        this.descendantNameFactory = descendantNameFactory;
        this.variableResolver = variableResolver;
        this.rdbmsModel = rdbmsModel;
        this.asmUtils = asmUtils;
        this.dialect = dialect;
        this.mappers = mapperFactory.getMappers(this);
        this.rules = rdbmsModel.getResource().getContents().stream()
                .filter(Rules.class::isInstance)
                .map(Rules.class::cast)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Rules not found in RDBMS model"));

    }

    @SuppressWarnings("unchecked")
    public Stream<RdbmsField> mapFeatureToRdbms(final ParameterType value, final EMap<Node, EList<EClass>> ancestors, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
        return mappers.entrySet().stream()
                .filter(c -> c.getKey().isAssignableFrom(value.getClass()))
                .flatMap(mapper -> ((RdbmsMapper<ParameterType>) mapper.getValue()).map(value, ancestors, parentIdFilterQuery, queryParameters));
    }

    public String getTableName(final EClass type) {
        return rdbmsResolver.rdbmsTable(type).getSqlName();
    }

    public String getColumnName(final EAttribute attribute) {
        return rdbmsResolver.rdbmsField(attribute).getSqlName();
    }

    public String getAncestorPostfix(final EClass clazz) {
        return ancestorNameFactory.getAncestorPostfix(clazz);
    }

    public String getDescendantPostfix(final EClass clazz) {
        return descendantNameFactory.getDescendantPostfix(clazz);
    }


    @Builder
    public class ProcessJoinParameters {
        @NonNull
        Node join;
        EMap<Node, EList<EClass>> ancestors;
        EMap<Node, EList<EClass>> descendants;
        SubSelect parentIdFilterQuery;

        boolean withoutFeatures;
        Map<String, Object> mask;
        Map<String, Object> queryParameters;
    }

    /**
     * Resolve logical JOIN and return RDBMS JOIN definition.
     *
     * @param params
     * @return RDBMS JOIN definition(s)
     */
    @SuppressWarnings("unchecked")
    public List<RdbmsJoin> processJoin(ProcessJoinParameters params) {
        if (params.join instanceof ReferencedJoin) {
            return processSimpleJoin("", (ReferencedJoin) params.join, ((ReferencedJoin) params.join).getReference(), ((ReferencedJoin) params.join).getReference().getEOpposite(), params.ancestors, params.descendants, params.parentIdFilterQuery, params.queryParameters);
        } else if (params.join instanceof ContainerJoin) {
            return processContainerJoin((ContainerJoin) params.join, params.ancestors, params.descendants, params.parentIdFilterQuery, params.queryParameters);
        } else if (params.join instanceof CastJoin) {
            return processCastJoin(params.join, params.parentIdFilterQuery, this, params.queryParameters);
        } else if (params.join instanceof SubSelectJoin) {
            final SubSelect subSelect = ((SubSelectJoin) params.join).getSubSelect();
            subSelect.getFilters().addAll(params.join.getFilters());

            final List<RdbmsJoin> result = new ArrayList<>();
            final Map<String, Object> _mask = params.mask != null && subSelect.getTransferRelation() != null ? (Map<String, Object>) params.mask.get(subSelect.getTransferRelation().getName()) : null;
            final RdbmsResultSet<ID> resultSetHandler =
                    RdbmsResultSet.<ID>builder()
                            .query(subSelect)
                            .filterByInstances(false)
                            .parentIdFilterQuery(params.parentIdFilterQuery)
                            .rdbmsBuilder(this)
                            .seek(null)
                            .withoutFeatures(params.withoutFeatures)
                            .mask(_mask)
                            .queryParameters(params.queryParameters)
                            .skipParents(false)
                            .build();

            if (!AsmUtils.equals(((SubSelectJoin) params.join).getPartner(), subSelect.getBase())) {
                result.add(RdbmsTableJoin.builder()
                        .tableName(getTableName(subSelect.getBase().getType()))
                        .columnName(StatementExecutor.ID_COLUMN_NAME)
                        .partnerTable(((SubSelectJoin) params.join).getPartner())
                        .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                        .alias(subSelect.getBase().getAlias())
                        .build());
            }

            result.add(RdbmsQueryJoin.<ID>builder()
                    .resultSet(resultSetHandler)
                    .outer(true)
                    .columnName(RdbmsAliasUtil.getOptionalParentIdColumnAlias(subSelect.getContainer()))
//                    .partnerTable(!subSelect.getNavigationJoins().isEmpty() && AsmUtils.equals(subSelect.getNavigationJoins().get(0).getPartner(), join) ? subSelect.getBase() : null)
//                    .partnerColumnName(!subSelect.getNavigationJoins().isEmpty() && AsmUtils.equals(subSelect.getNavigationJoins().get(0).getPartner(), join) ? StatementExecutor.ID_COLUMN_NAME : null)
                    .partnerTable(subSelect.getBase())
                    .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                    .alias(subSelect.getAlias())
                    .build());
            final Optional<Feature> selectorFeature = subSelect.getSelect().getFeatures().stream()
                    .filter(f -> (f instanceof Function) && (FunctionSignature.MIN_INTEGER.equals(((Function) f).getSignature()) || FunctionSignature.MAX_INTEGER.equals(((Function) f).getSignature())))
                    .findAny();
            checkArgument(selectorFeature.isPresent(), "SubSelectFeature of head/tail/any must exists");
            final Optional<RdbmsMapper.RdbmsTarget> selectorTarget = RdbmsMapper.getTargets(selectorFeature.get()).findAny();
            checkArgument(selectorTarget.isPresent(), "SubSelectFeature target must exists");
            result.add(RdbmsTableJoin.builder()
                    .tableName(rdbmsResolver.rdbmsTable(params.join.getType()).getSqlName())
                    .alias(params.join.getAlias())
                    .columnName(StatementExecutor.ID_COLUMN_NAME)
                    .partnerTable(subSelect)
                    .partnerColumnName(selectorTarget.get().getAlias() + "_" + selectorTarget.get().getTarget().getIndex())
                    .outer(true)
                    .build());

            if (params.ancestors.containsKey(params.join)) {
                result.addAll(getAncestorJoins(params.join, params.ancestors, result));
//                result.addAll(ancestors.get(join).stream()
//                        .flatMap(ancestor -> getAncestorJoins(join, ancestors, result).stream())
//                        .collect(Collectors.toList()));
            }
            if (params.descendants.containsKey(params.join)) {
                result.addAll(getDescendantJoins(params.join, params.descendants, result));

//                result.addAll(descendants.get(join).stream()
//                        .flatMap(descendant -> getDescendantJoins(join, descendants, result).stream())
//                        .collect(Collectors.toList()));
            }

            return result;
        } else if (params.join instanceof CustomJoin) {
            final List<RdbmsJoin> result = new ArrayList<>();

            final CustomJoin customJoin = (CustomJoin) params.join;

            final String sql;
            if (customJoin.getNavigationSql().indexOf('`') != -1) {
                sql = resolveRdbmsNames(customJoin.getNavigationSql());
            } else {
                sql = customJoin.getNavigationSql();
            }

            result.add(RdbmsCustomJoin.builder()
                    .sql(sql)
                    .sourceIdSetParameterName(customJoin.getSourceIdSetParameter())
                    .alias(params.join.getAlias())
                    .columnName(customJoin.getSourceIdParameter())
                    .partnerTable(customJoin.getPartner())
                    .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                    .outer(true)
                    .build());

            if (params.ancestors.containsKey(params.join)) {
                result.addAll(getAncestorJoins(params.join, params.ancestors, result));
//                ancestors.get(join).forEach(ancestor ->
//                        result.addAll(getAncestorJoins(join, ancestors, result)));
            }
            if (params.descendants.containsKey(params.join)) {
                result.addAll(getDescendantJoins(params.join, params.descendants, result));
//                descendants.get(join).forEach(descendant ->
//                        result.addAll(getDescendantJoins(join, descendants, result)));
            }

            return result;
        } else {
            throw new IllegalStateException("Invalid JOIN");
        }
    }

    private String resolveRdbmsNames(final String sql) {
        final StringBuilder result = new StringBuilder();
        final StringCharacterIterator it = new StringCharacterIterator(sql);

        boolean resolving = false;
        StringBuilder fqNameBuilder = null;
        for (char ch = it.first(); ch != CharacterIterator.DONE; ch = it.next()) {
            if (ch == '`' && resolving) {
                final String fqName = fqNameBuilder.toString();

                final Optional<EAttribute> attribute = asmUtils.resolveAttribute(fqName);
                final Optional<EReference> reference = attribute.isPresent() ? Optional.empty() : asmUtils.resolveReference(fqName);
                final Optional<EClass> type = attribute.isPresent() || reference.isPresent() ? Optional.empty() : asmUtils.resolve(fqName).filter(c -> c instanceof EClass).map(c -> (EClass) c);

                // TODO - support resolving junction table names
                if (attribute.isPresent()) {
                    result.append(rdbmsResolver.rdbmsField(attribute.get()).getSqlName());
                } else if (reference.isPresent()) {
                    result.append(rdbmsResolver.rdbmsField(reference.get()).getSqlName());
                } else if (type.isPresent()) {
                    result.append(rdbmsResolver.rdbmsTable(type.get()).getSqlName());
                } else {
                    throw new IllegalStateException("Unable to resolve ASM element name: " + fqName);
                }

                resolving = false;
            } else if (ch == '`' && !resolving) {
                fqNameBuilder = new StringBuilder();
                resolving = true;
            } else if (resolving) {
                fqNameBuilder.append(ch);
            } else {
                result.append(ch);
            }
        }

        if (resolving) {
            log.error("SQL syntax is invalid (terminated while resolving RDBMS name): {}", sql);
            throw new IllegalArgumentException("Invalid custom SQL");
        }

        return result.toString();
    }

    @SuppressWarnings("unchecked")
    private List<RdbmsJoin> processSimpleJoin(final String postfix, final Join join, final EReference reference, final EReference opposite, final EMap<Node, EList<EClass>> ancestors, final EMap<Node, EList<EClass>> descendants, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
        final EClass targetType = join.getType();
        final Node node = join.getPartner();
        final EClass sourceType = node.getType();

        if (log.isTraceEnabled()) {
            log.trace(" => processing JOIN: {}", join);
            log.trace("    target type: {}", targetType.getName());
            log.trace("    source type: {}", sourceType.getName());
        }

        final String tableName = rdbmsResolver.rdbmsTable(targetType).getSqlName();

        // create builder for RDBMS JOIN definition
        @SuppressWarnings("rawtypes")
        final RdbmsJoin.RdbmsJoinBuilder builder = RdbmsTableJoin.builder()
                .outer(true)
                .tableName(tableName)
                .alias(join.getAlias() + postfix)
                .partnerTable(node);

        final Rule rule;
        EClass sourceContainer = null;
        if (reference != null) {
            // get RDBMS rule of a given reference
            rule = rules.getRuleFromReference(reference);

            sourceContainer = reference.getEContainingClass();
            if (log.isTraceEnabled()) {
                log.trace("    reference: {}", reference.getName());
                log.trace("    reference container: {}", sourceContainer.getName());
            }
        } else {
            rule = null;
        }

        final Rule oppositeRule;
        if (opposite != null) {
            oppositeRule = rules.getRuleFromReference(opposite);

            final EClass oppositeContainer = opposite.getEReferenceType();
            if (log.isTraceEnabled()) {
                log.trace("    opposite: {}", opposite.getName());
                log.trace("    opposite reference container: {}", oppositeContainer.getName());
            }

            if (sourceContainer == null) {
                sourceContainer = oppositeContainer;
            }
        } else {
            oppositeRule = null;
        }

        if (!AsmUtils.equals(sourceType, sourceContainer)) { // reference is inherited from another class, resolve ancestor too
            log.trace("  - reference '{}' is inherited");

            if (!ancestors.containsKey(node)) {
                ancestors.put(node, new UniqueEList<>());
            }
            ancestors.get(node).add(sourceContainer);
            builder.partnerTablePostfix(getAncestorPostfix(sourceContainer));
        }
        if (!targetType.getEAllSuperTypes().isEmpty()) {
            if (!ancestors.containsKey(join)) {
                ancestors.put(join, new UniqueEList<>());
            }
            ancestors.get(join).addAll(targetType.getEAllSuperTypes());
        }

        if (rule != null && rule.isForeignKey()) { // reference is owned by source class, target class has reference to the ID with different name
            log.trace("  - reference '{}' is foreign key", reference.getName());

            builder.columnName(StatementExecutor.ID_COLUMN_NAME).partnerColumnName(rdbmsResolver.rdbmsField(reference).getSqlName());
        } else if (rule != null && rule.isInverseForeignKey()) {  // reference is owned by target class, source class has reference to the ID with different name
            log.trace("  - reference '{}' is inverse foreign key", reference.getName());

            builder.columnName(rdbmsResolver.rdbmsField(reference).getSqlName()).partnerColumnName(StatementExecutor.ID_COLUMN_NAME);
        } else if (rule != null && rule.isJoinTable()) { // JOIN tables are not supported yet
            log.trace("  - reference '{}' is JOIN table", reference.getName());

            builder.columnName(StatementExecutor.ID_COLUMN_NAME).partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                    .junctionTableName(rdbmsResolver.rdbmsJunctionTable(reference).getSqlName())
                    .junctionColumnName(rdbmsResolver.rdbmsJunctionField(reference).getSqlName())
                    .junctionOppositeColumnName(rdbmsResolver.rdbmsJunctionOppositeField(reference).getSqlName());
        } else if (oppositeRule != null && oppositeRule.isForeignKey()) { // reference is owned by source class, target class has reference to the ID with different name (defined by opposite reference)
            log.trace("  - opposite reference '{}' is foreign key", opposite.getName());

            builder.columnName(rdbmsResolver.rdbmsField(opposite).getSqlName()).partnerColumnName(StatementExecutor.ID_COLUMN_NAME);
        } else if (oppositeRule != null && oppositeRule.isInverseForeignKey()) {  // reference is owned by target class, source class has reference to the ID with different name (defined by opposite reference)
            log.trace("  - opposite reference '{}' is inverse foreign key", opposite.getName());

            builder.columnName(StatementExecutor.ID_COLUMN_NAME).partnerColumnName(rdbmsResolver.rdbmsField(opposite).getSqlName());
        } else if (oppositeRule != null && oppositeRule.isJoinTable()) { // JOIN tables are not supported yet
            log.trace("  - opposite reference '{}' is JOIN table", opposite.getName());

            builder.columnName(StatementExecutor.ID_COLUMN_NAME).partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                    .junctionTableName(rdbmsResolver.rdbmsJunctionTable(opposite).getSqlName())
                    .junctionColumnName(rdbmsResolver.rdbmsJunctionField(opposite).getSqlName())
                    .junctionOppositeColumnName(rdbmsResolver.rdbmsJunctionOppositeField(opposite).getSqlName());
        } else {
            throw new IllegalStateException("Invalid reference");
        }

        if (!join.getFilters().isEmpty() && join.getFilters().stream().noneMatch(filter -> filter.getFeatures().stream().anyMatch(feature -> feature instanceof SubSelectFeature))) {
            builder.onConditions(join.getFilters().stream()
                    .map(f -> RdbmsFunction.builder()
                            .pattern("EXISTS ({0})")
                            .parameter(
                                    RdbmsNavigationFilter.<ID>builder()
                                            .filter(f)
                                            .rdbmsBuilder(this)
                                            .parentIdFilterQuery(parentIdFilterQuery)
                                            .queryParameters(queryParameters)
                                            .build())
                            .build())
                    .collect(Collectors.toList()));
        }

        final RdbmsJoin rdbmsJoin = builder.build();

        final List<RdbmsJoin> result = new ArrayList<>();
        result.add(rdbmsJoin);

        if (ancestors.containsKey(join)) {
            result.addAll(getAncestorJoins(join, ancestors, result));
//            ancestors.get(join).stream().forEach(ancestor ->
//                    result.addAll(getAncestorJoins(join, ancestors, result)));
        }
        if (descendants.containsKey(join)) {
            result.addAll(getDescendantJoins(join, descendants, result));
//            descendants.get(join).stream().forEach(ancestor ->
//                    result.addAll(getDescendantJoins(join, descendants, result)));
        }

        return result;
    }

    private List<RdbmsJoin> processContainerJoin(final ContainerJoin join, final EMap<Node, EList<EClass>> ancestors,
                                                 final EMap<Node, EList<EClass>> descendants, final SubSelect parentIdFilterQuery,
                                                 final Map<String, Object> queryParameters) {
        final EClass targetType = join.getType();
        final Node node = join.getPartner();
        final EList<EReference> references = join.getReferences();
        final EClass sourceType = node != null ? node.getType() : references.get(0).getEReferenceType();

        if (log.isTraceEnabled()) {
            log.trace(" => processing JOIN: {}", join);
            log.trace("    target type: {}", targetType.getName());
            log.trace("    source type: {}", sourceType.getName());
            log.trace("    references: {}", references.stream().map(r -> AsmUtils.getReferenceFQName(r)).collect(Collectors.joining(", ")));
        }

        final List<RdbmsJoin> result = new ArrayList<>();
        int index = 0;
        for (final EReference r : join.getReferences()) {
            result.addAll(processSimpleJoin(RdbmsContainerJoin.POSTFIX + index++, join, null, r, ancestors, descendants, parentIdFilterQuery, queryParameters));
        }

        result.add(RdbmsContainerJoin.builder()
                .outer(true)
                .tableName(rdbmsResolver.rdbmsTable(targetType).getSqlName())
                .alias(join.getAlias())
                .partnerTable(node)
                .columnName(StatementExecutor.ID_COLUMN_NAME)
                .references(references)
                .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                .build());

        return result;
    }

    private List<RdbmsJoin> processCastJoin(Node join, SubSelect parentIdFilterQuery, RdbmsBuilder<ID> rdbmsBuilder, Map<String, Object> queryParameters) {
        EClass castTargetType = join.getType();
        Set<EClass> typeSet = new HashSet<>(castTargetType.getEAllSuperTypes());
        typeSet.add(castTargetType);

        List<EClass> types = typeSet.stream()
                                    .sorted((l, r) -> {
                                        // ascending order
                                        // type < supertype
                                        if (l.getEAllSuperTypes().contains(r)) {
                                            return -1;
                                        }
                                        if (r.getEAllSuperTypes().contains(l)) {
                                            return 1;
                                        }
                                        return 0;
                                    })
                                    .collect(Collectors.toUnmodifiableList());

        List<RdbmsJoin> rdbmsTableJoins = new ArrayList<>();
        for (EClass type : types) {
            String alias = join.getAlias();
            Node partnerTable = ((CastJoin) join).getPartner();
            List<RdbmsFunction> onConditions = new ArrayList<>();
            RdbmsTableJoin rdbmsPartnerTable = null;

            if (rdbmsTableJoins.isEmpty()) {
                // original, first join
                List<Filter> joinFilters = join.getFilters();
                boolean joinFiltersWithoutSubSelectFeatures =
                        !joinFilters.isEmpty()
                        && joinFilters.stream()
                                      .noneMatch(filter -> filter.getFeatures().stream()
                                                                 .anyMatch(feature -> feature instanceof SubSelectFeature));
                if (joinFiltersWithoutSubSelectFeatures) {
                    onConditions = joinFilters.stream()
                                              .map(f -> RdbmsFunction.builder()
                                                                     .pattern("EXISTS ({0})")
                                                                     .parameter(RdbmsNavigationFilter.<ID>builder()
                                                                                                     .filter(f)
                                                                                                     .rdbmsBuilder(this)
                                                                                                     .queryParameters(queryParameters)
                                                                                                     .parentIdFilterQuery(parentIdFilterQuery)
                                                                                                     .build())
                                                                     .build())
                                              .collect(Collectors.toList());
                }
            } else {
                // additional join for supertypes
                alias += rdbmsBuilder.getAncestorPostfix(type);
                partnerTable = null;
                rdbmsPartnerTable = (RdbmsTableJoin) rdbmsTableJoins.get(rdbmsTableJoins.size() - 1);
            }

            rdbmsTableJoins.add(RdbmsTableJoin.builder()
                                              .outer(true)
                                              .alias(alias)
                                              .partnerTable(partnerTable)
                                              .onConditions(onConditions)
                                              .rdbmsPartnerTable(rdbmsPartnerTable)
                                              .columnName(SelectStatementExecutor.ID_COLUMN_NAME)
                                              .tableName(rdbmsResolver.rdbmsTable(type).getSqlName())
                                              .partnerColumnName(SelectStatementExecutor.ID_COLUMN_NAME)
                                              .build());
        }

        return rdbmsTableJoins;
    }

    public Collection<RdbmsJoin> getAncestorJoins(final Node node, final EMap<Node, EList<EClass>> ancestors, final Collection<RdbmsJoin> joins) {
        final EList<EClass> list;
        if (ancestors.containsKey(node)) {
            list = ancestors.get(node);
        } else if (node.eContainer() instanceof SubSelect && ancestors.containsKey(((SubSelect) node.eContainer()).getSelect())) {
            list = ancestors.get(((SubSelect) node.eContainer()).getSelect());
        } else if (node.eContainer() instanceof Node && ancestors.containsKey(node.eContainer())) {
            list = ancestors.get(node.eContainer());
        } else {
            list = ECollections.emptyEList();
        }

        return list.stream()
                .filter(c -> joins.stream().noneMatch(j -> Objects.equals(node.getAlias() + getAncestorPostfix(c), j.getAlias())))
                .map(ancestor -> RdbmsTableJoin.builder()
                        .tableName(rdbmsResolver.rdbmsTable(ancestor).getSqlName())
                        .alias(node.getAlias() + getAncestorPostfix(ancestor))
                        .columnName(StatementExecutor.ID_COLUMN_NAME)
                        .partnerTable(node)
                        .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                        .outer(true)
                        .build())
                .collect(Collectors.toList());
    }

    public Collection<RdbmsJoin> getDescendantJoins(final Node node, final EMap<Node, EList<EClass>> descendants, final Collection<RdbmsJoin> joins) {
        final EList<EClass> list;
        if (descendants.containsKey(node)) {
            list = descendants.get(node);
        } else if (node.eContainer() instanceof SubSelect && descendants.containsKey(((SubSelect) node.eContainer()).getSelect())) {
            list = descendants.get(((SubSelect) node.eContainer()).getSelect());
        } else if (node.eContainer() instanceof Node && descendants.containsKey(node.eContainer())) {
            list = descendants.get(node.eContainer());
        } else {
            list = ECollections.emptyEList();
        }

        return list.stream()
                .filter(c -> joins.stream().noneMatch(j -> Objects.equals(node.getAlias() + getAncestorPostfix(c), j.getAlias())))
                .map(ancestor -> RdbmsTableJoin.builder()
                        .tableName(rdbmsResolver.rdbmsTable(ancestor).getSqlName())
                        .alias(node.getAlias() + getAncestorPostfix(ancestor))
                        .columnName(StatementExecutor.ID_COLUMN_NAME)
                        .partnerTable(node)
                        .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                        .outer(true)
                        .build())
                .collect(Collectors.toList());
    }

    public ThreadLocal<Map<String, Collection<? extends RdbmsField>>> getConstantFields() {
        return CONSTANT_FIELDS;
    }
}
