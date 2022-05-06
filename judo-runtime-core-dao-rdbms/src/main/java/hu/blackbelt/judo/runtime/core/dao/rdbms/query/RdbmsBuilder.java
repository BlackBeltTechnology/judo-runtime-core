package hu.blackbelt.judo.runtime.core.dao.rdbms.query;

import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.meta.rdbms.support.RdbmsModelResourceSupport;
import hu.blackbelt.judo.meta.rdbmsRules.Rule;
import hu.blackbelt.judo.meta.rdbmsRules.Rules;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.executors.StatementExecutor;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.ECollections;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.common.util.UniqueEList;
import org.eclipse.emf.ecore.*;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.meta.rdbms.support.RdbmsModelResourceSupport.rdbmsModelResourceSupportBuilder;

@Builder(builderClassName = "RdbmsInternalBuilder")
@Slf4j
public class RdbmsBuilder {

    private RdbmsResolver rdbmsResolver;

    @Getter
    private RdbmsParameterMapper parameterMapper;

    @Getter
    private Coercer coercer;

    @Getter
    private final AtomicInteger constantCounter = new AtomicInteger(0);

    @Getter
    private AncestorNameFactory ancestorNameFactory;

    @Getter
    private Dialect dialect;

    @Getter
    private VariableResolver variableResolver;

    private RdbmsModel rdbmsModel;

    @Getter
    private AsmUtils asmUtils;

    private final Map<Class, RdbmsMapper> mappers = new HashMap<>();

    private Rules rules;

    private final ThreadLocal<Map<String, Collection<? extends RdbmsField>>> CONSTANT_FIELDS = new ThreadLocal<>();

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends RdbmsInternalBuilder {
        @Override
        public RdbmsBuilder build() {
            final RdbmsBuilder rdbmsBuilder = super.build();
            rdbmsBuilder.initMappers();
            return rdbmsBuilder;
        }
    }

    private void initMappers() {
        mappers.put(Attribute.class, new AttributeMapper(this));
        mappers.put(Constant.class, new ConstantMapper(this));
        mappers.put(Variable.class, new VariableMapper(this));
        mappers.put(Function.class, new FunctionMapper(this));
        mappers.put(IdAttribute.class, new IdAttributeMapper());
        mappers.put(TypeAttribute.class, new TypeAttributeMapper());
        mappers.put(EntityTypeName.class, new EntityTypeNameMapper(this));
        mappers.put(SubSelect.class, new SubSelectMapper(this));
        mappers.put(SubSelectFeature.class, new SubSelectFeatureMapper());

        final RdbmsModelResourceSupport rdbmsSupport = rdbmsModelResourceSupportBuilder().resourceSet(rdbmsModel.getResourceSet()).build();

        rules = rdbmsModel.getResource().getContents().stream()
                .filter(Rules.class::isInstance)
                .map(Rules.class::cast)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Rules not found in RDBMS model"));
    }

    public Stream<RdbmsField> mapFeatureToRdbms(final ParameterType value, final EMap<Node, EList<EClass>> ancestors, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
        return mappers.entrySet().stream()
                .filter(c -> c.getKey().isAssignableFrom(value.getClass()))
                .flatMap(mapper -> mapper.getValue().map(value, ancestors, parentIdFilterQuery, queryParameters));
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

    /**
     * Resolve logical JOIN and return RDBMS JOIN definition.
     *
     * @param join                logical JOIN
     * @param ancestors           ancestors of joined type
     * @param parentIdFilterQuery
     * @param rdbmsBuilder
     * @param mask
     * @return RDBMS JOIN definition(s)
     */
    public List<RdbmsJoin> processJoin(final Node join, final EMap<Node, EList<EClass>> ancestors, final SubSelect parentIdFilterQuery, final RdbmsBuilder rdbmsBuilder, final boolean withoutFeatures, final Map<String, Object> mask, final Map<String, Object> queryParameters) {
        if (join instanceof ReferencedJoin) {
            return processSimpleJoin("", (ReferencedJoin) join, ((ReferencedJoin) join).getReference(), ((ReferencedJoin) join).getReference().getEOpposite(), ancestors, parentIdFilterQuery, queryParameters);
        } else if (join instanceof ContainerJoin) {
            return processContainerJoin((ContainerJoin) join, ancestors, parentIdFilterQuery, queryParameters);
        } else if (join instanceof CastJoin) {
            RdbmsTableJoin.RdbmsJoinBuilder builder = RdbmsTableJoin.builder()
                    .tableName(rdbmsResolver.rdbmsTable(join.getType()).getSqlName())
                    .alias(join.getAlias())
                    .columnName(StatementExecutor.ID_COLUMN_NAME)
                    .partnerTable(((CastJoin) join).getPartner())
                    .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                    .outer(true);
            if (!join.getFilters().isEmpty() && join.getFilters().stream().noneMatch(filter -> filter.getFeatures().stream().anyMatch(feature -> feature instanceof SubSelectFeature))) {
                builder.onConditions(join.getFilters().stream()
                        .map(f -> RdbmsFunction.builder()
                                .pattern("EXISTS ({0})")
                                .parameter(new RdbmsNavigationFilter(f, this, parentIdFilterQuery, queryParameters))
                                .build())
                        .collect(Collectors.toList()));
            }
            return Collections.singletonList(builder
                    .build());
        } else if (join instanceof SubSelectJoin) {
            final SubSelect subSelect = ((SubSelectJoin) join).getSubSelect();
            subSelect.getFilters().addAll(join.getFilters());

            final List<RdbmsJoin> result = new ArrayList<>();
            final Map<String, Object> _mask = mask != null && subSelect.getTransferRelation() != null ? (Map<String, Object>) mask.get(subSelect.getTransferRelation().getName()) : null;
            final RdbmsResultSet resultSetHandler = new RdbmsResultSet(subSelect, false, parentIdFilterQuery, rdbmsBuilder, null, withoutFeatures, _mask, queryParameters, false);

            if (!AsmUtils.equals(((SubSelectJoin) join).getPartner(), subSelect.getBase())) {
                result.add(RdbmsTableJoin.builder()
                        .tableName(rdbmsBuilder.getTableName(subSelect.getBase().getType()))
                        .columnName(StatementExecutor.ID_COLUMN_NAME)
                        .partnerTable(((SubSelectJoin) join).getPartner())
                        .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                        .alias(subSelect.getBase().getAlias())
                        .build());
            }

            result.add(RdbmsQueryJoin.builder()
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
                    .tableName(rdbmsResolver.rdbmsTable(join.getType()).getSqlName())
                    .alias(join.getAlias())
                    .columnName(StatementExecutor.ID_COLUMN_NAME)
                    .partnerTable(subSelect)
                    .partnerColumnName(selectorTarget.get().getAlias() + "_" + selectorTarget.get().getTarget().getIndex())
                    .outer(true)
                    .build());

            if (ancestors.containsKey(join)) {
                result.addAll(ancestors.get(join).stream()
                        .flatMap(ancestor -> getAdditionalJoins(join, ancestors, result).stream())
                        .collect(Collectors.toList()));
            }
            return result;
        } else if (join instanceof CustomJoin) {
            final List<RdbmsJoin> result = new ArrayList<>();

            final CustomJoin customJoin = (CustomJoin) join;

            final String sql;
            if (customJoin.getNavigationSql().indexOf('`') != -1) {
                sql = resolveRdbmsNames(customJoin.getNavigationSql());
            } else {
                sql = customJoin.getNavigationSql();
            }

            result.add(RdbmsCustomJoin.builder()
                    .sql(sql)
                    .sourceIdSetParameterName(customJoin.getSourceIdSetParameter())
                    .alias(join.getAlias())
                    .columnName(customJoin.getSourceIdParameter())
                    .partnerTable(customJoin.getPartner())
                    .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                    .outer(true)
                    .build());

            if (ancestors.containsKey(join)) {
                ancestors.get(join).forEach(ancestor ->
                        result.addAll(getAdditionalJoins(join, ancestors, result)));
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

    private List<RdbmsJoin> processSimpleJoin(final String postfix, final Join join, final EReference reference, final EReference opposite, final EMap<Node, EList<EClass>> ancestors, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
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
                            .parameter(new RdbmsNavigationFilter(f, this, parentIdFilterQuery, queryParameters))
                            .build())
                    .collect(Collectors.toList()));
        }

        final RdbmsJoin rdbmsJoin = builder.build();

        final List<RdbmsJoin> result = new ArrayList<>();
        result.add(rdbmsJoin);

        if (ancestors.containsKey(join)) {
            ancestors.get(join).stream().forEach(ancestor ->
                    result.addAll(getAdditionalJoins(join, ancestors, result)));
        }

        return result;
    }

    private List<RdbmsJoin> processContainerJoin(final ContainerJoin join, final EMap<Node, EList<EClass>> ancestors, final SubSelect parentIdFilterQuery, final Map<String, Object> queryParameters) {
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
            result.addAll(processSimpleJoin(RdbmsContainerJoin.POSTFIX + index++, join, null, r, ancestors, parentIdFilterQuery, queryParameters));
        }

        final String tableName = rdbmsResolver.rdbmsTable(targetType).getSqlName();

        result.add(RdbmsContainerJoin.builder()
                .outer(true)
                .tableName(tableName)
                .alias(join.getAlias())
                .partnerTable(node)
                .columnName(StatementExecutor.ID_COLUMN_NAME)
                .references(references)
                .partnerColumnName(StatementExecutor.ID_COLUMN_NAME)
                .build());

        return result;
    }

    public Collection<RdbmsJoin> getAdditionalJoins(final Node node, final EMap<Node, EList<EClass>> ancestors, final Collection<RdbmsJoin> joins) {
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
                .filter(ancestor -> joins.stream().noneMatch(j -> Objects.equals(node.getAlias() + getAncestorPostfix(ancestor), j.getAlias())))
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
