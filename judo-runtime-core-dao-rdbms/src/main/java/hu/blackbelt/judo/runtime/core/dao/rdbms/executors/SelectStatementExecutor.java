package hu.blackbelt.judo.runtime.core.dao.rdbms.executors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.FileType;
import hu.blackbelt.judo.dispatcher.api.VariableResolver;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.AttributeSelector;
import hu.blackbelt.judo.meta.expression.LogicalExpression;
import hu.blackbelt.judo.meta.expression.TypeName;
import hu.blackbelt.judo.meta.expression.builder.jql.JqlExpressionBuilder;
import hu.blackbelt.judo.meta.expression.builder.jql.JqlExpressionBuildingContext;
import hu.blackbelt.judo.meta.expression.constant.*;
import hu.blackbelt.judo.meta.expression.logical.*;
import hu.blackbelt.judo.meta.expression.object.ObjectVariableReference;
import hu.blackbelt.judo.meta.expression.support.ExpressionModelResourceSupport;
import hu.blackbelt.judo.meta.jql.jqldsl.JqlExpression;
import hu.blackbelt.judo.meta.jql.runtime.JqlParser;
import hu.blackbelt.judo.meta.measure.Measure;
import hu.blackbelt.judo.meta.measure.Unit;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.query.*;
import hu.blackbelt.judo.meta.rdbms.runtime.RdbmsModel;
import hu.blackbelt.judo.runtime.core.DataTypeManager;
import hu.blackbelt.judo.runtime.core.MetricsCancelToken;
import hu.blackbelt.judo.runtime.core.MetricsCollector;
import hu.blackbelt.judo.runtime.core.dao.rdbms.Dialect;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsParameterMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.RdbmsResolver;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.AncestorNameFactory;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.RdbmsBuilder;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.mappers.RdbmsMapper;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.model.RdbmsResultSet;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.translators.*;
import hu.blackbelt.judo.runtime.core.dao.rdbms.query.utils.RdbmsAliasUtil;
import hu.blackbelt.judo.runtime.core.query.Context;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.tatami.core.TransformationTraceService;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.mapper.api.ExtendableCoercer;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.common.util.*;
import org.eclipse.emf.ecore.*;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static hu.blackbelt.judo.meta.expression.constant.util.builder.ConstantBuilders.newInstanceBuilder;
import static hu.blackbelt.judo.meta.query.util.builder.QueryBuilders.*;

@Slf4j
public class SelectStatementExecutor<ID> extends StatementExecutor<ID> {

    private final MeasureModel measureModel;

    private final Translator translator = new Translator();
    private final MetricsCollector metricsCollector;

    private final RdbmsBuilder rdbmsBuilder;

    private final QueryFactory queryFactory;

    private final DataTypeManager dataTypeManager;

    private static final String METRICS_SELECT_PREPARE = "select-prepare";
    private static final String METRICS_SELECT_PROCESSING = "select-processing";
    private static final String METRICS_SELECT_QUERY = "select-query";

    private final int chunkSize;

    private final AsmUtils asmUtils;

    @Builder
    public SelectStatementExecutor(@NonNull final AsmModel asmModel,
                                   @NonNull final RdbmsModel rdbmsModel,
                                   @NonNull final MeasureModel measureModel,
                                   @NonNull final TransformationTraceService transformationTraceService,
                                   @NonNull final QueryFactory queryFactory,
                                   @NonNull final RdbmsParameterMapper rdbmsParameterMapper,
                                   @NonNull final RdbmsResolver rdbmsResolver,
                                   @NonNull final DataTypeManager dataTypeManager,
                                   @NonNull final IdentifierProvider<ID> identifierProvider,
                                   @NonNull final RdbmsBuilder rdbmsBuilder,
                                   @NonNull final MetricsCollector metricsCollector,
                                   @NonNull final int chunkSize) {
        super(asmModel, rdbmsModel, transformationTraceService, rdbmsParameterMapper, rdbmsResolver, dataTypeManager.getCoercer(),
                identifierProvider);
        this.measureModel = measureModel;
        this.queryFactory = queryFactory;
        this.dataTypeManager = dataTypeManager;
        this.metricsCollector = metricsCollector;
        this.chunkSize = chunkSize;

        asmUtils = new AsmUtils(asmModel.getResourceSet());

        this.rdbmsBuilder = rdbmsBuilder;
        translator.getTranslators().put(MeasuredDecimal.class, MeasuredDecimalConstantTranslator.builder().build());
        translator.getTranslators().put(AttributeSelector.class, AttributeSelectorTranslator.builder().translator(translator).asmUtils(asmUtils).asmModelAdapter(queryFactory.getModelAdapter()).queryFactory(queryFactory).build());
        translator.getTranslators().put(BooleanConstant.class, BooleanConstantTranslator.builder().build());
        translator.getTranslators().put(CustomData.class, CustomDataTranslator.builder().build());
        translator.getTranslators().put(DateComparison.class, DateComparisonTranslator.builder().translator(translator).build());
        translator.getTranslators().put(DateConstant.class, DateConstantTranslator.builder().build());
        translator.getTranslators().put(DecimalComparison.class, DecimalComparisonTranslator.builder().translator(translator).build());
        translator.getTranslators().put(DecimalConstant.class, DecimalConstantTranslator.builder().build());
        translator.getTranslators().put(EnumerationComparison.class, EnumerationComparisonTranslator.builder().translator(translator).build());
        translator.getTranslators().put(Instance.class, InstanceTranslator.builder().asmUtils(asmUtils).build());
        translator.getTranslators().put(IntegerComparison.class, IntegerComparisonTranslator.builder().translator(translator).build());
        translator.getTranslators().put(IntegerConstant.class, IntegerConstantTranslator.builder().build());
        translator.getTranslators().put(KleeneExpression.class, KleeneTranslator.builder().translator(translator).build());
        translator.getTranslators().put(Literal.class, LiteralTranslator.builder().build());
        translator.getTranslators().put(Matches.class, MatchesTranslator.builder().translator(translator).build());
        translator.getTranslators().put(Like.class, LikeTranslator.builder().translator(translator).build());
        translator.getTranslators().put(NegationExpression.class, NegationTranslator.builder().translator(translator).build());
        translator.getTranslators().put(ObjectVariableReference.class, ObjectVariableReferenceTranslator.builder().translator(translator).build());
        translator.getTranslators().put(StringComparison.class, StringComparisonTranslator.builder().translator(translator).build());
        translator.getTranslators().put(StringConstant.class, StringConstantTranslator.builder().build());
        translator.getTranslators().put(TimestampComparison.class, TimestampComparisonTranslator.builder().translator(translator).build());
        translator.getTranslators().put(TimestampConstant.class, TimestampConstantTranslator.builder().build());
        translator.getTranslators().put(TimeComparison.class, TimeComparisonTranslator.builder().translator(translator).build());
        translator.getTranslators().put(TimeConstant.class, TimeConstantTranslator.builder().build());
    }

    public Optional<Payload> selectMetadata(final NamedParameterJdbcTemplate jdbcTemplate, final EClass mappedTransferObjectType, final ID id) {
        rdbmsBuilder.getConstantFields().set(new HashMap<>());

        EClass entityType;
        final MapSqlParameterSource parameters = new MapSqlParameterSource();

        try (MetricsCancelToken ct = metricsCollector.start(METRICS_SELECT_PREPARE)) {
            entityType = asmUtils.getMappedEntityType(mappedTransferObjectType)
                    .orElseThrow(() -> new IllegalStateException("Invalid mapped transfer object type"));

            parameters.addValue("id", getCoercer().coerce(id, rdbmsBuilder.getParameterMapper().getIdClassName()), rdbmsBuilder.getParameterMapper().getIdSqlType());
        }

        final Collection<Map<String, Object>> results;
        try (MetricsCancelToken ct = metricsCollector.start(METRICS_SELECT_QUERY)) {
            results = jdbcTemplate.queryForList("SELECT " + StatementExecutor.ID_COLUMN_NAME + ", " +
                    StatementExecutor.ENTITY_TYPE_COLUMN_NAME + ", " +
                    StatementExecutor.ENTITY_VERSION_COLUMN_NAME + ", " +
                    StatementExecutor.ENTITY_CREATE_USERNAME_COLUMN_NAME + ", " +
                    StatementExecutor.ENTITY_CREATE_USER_ID_COLUMN_NAME + ", " +
                    StatementExecutor.ENTITY_CREATE_TIMESTAMP_COLUMN_NAME + ", " +
                    StatementExecutor.ENTITY_UPDATE_USERNAME_COLUMN_NAME + ", " +
                    StatementExecutor.ENTITY_UPDATE_USER_ID_COLUMN_NAME + ", " +
                    StatementExecutor.ENTITY_UPDATE_TIMESTAMP_COLUMN_NAME +
                    "\nFROM " + rdbmsBuilder.getTableName(entityType) +
                    "\nWHERE " + StatementExecutor.ID_COLUMN_NAME + " = :id", parameters);
        }

        if (!results.isEmpty()) {
            try (MetricsCancelToken ct = metricsCollector.start(METRICS_SELECT_PROCESSING)) {
                final Map<String, Object> record = results.iterator().next();

                final java.util.function.Function<String, Object> extract = key -> record.entrySet().stream()
                        .filter(e -> key.equalsIgnoreCase(e.getKey()) && e.getValue() != null)
                        .map(e -> e.getValue())
                        .findAny()
                        .orElse(null);

                final Object idInRecord = extract.apply(StatementExecutor.ID_COLUMN_NAME);
                final Object entityTypeInRecord = extract.apply(StatementExecutor.ENTITY_TYPE_COLUMN_NAME);
                final Object entityVersionInRecord = extract.apply(StatementExecutor.ENTITY_VERSION_COLUMN_NAME);
                final Object entityCreateUsernameInRecord = extract.apply(StatementExecutor.ENTITY_CREATE_USER_ID_COLUMN_NAME);
                final Object entityCreateUserIdInRecord = extract.apply(StatementExecutor.ENTITY_CREATE_USER_ID_COLUMN_NAME);
                final Object entityCreateTimestampInRecord = extract.apply(StatementExecutor.ENTITY_CREATE_TIMESTAMP_COLUMN_NAME);
                final Object entityUpdateUsernameInRecord = extract.apply(StatementExecutor.ENTITY_UPDATE_USERNAME_COLUMN_NAME);
                final Object entityUpdateUserIdInRecord = extract.apply(StatementExecutor.ENTITY_UPDATE_USER_ID_COLUMN_NAME);
                final Object entityUpdateTimestampInRecord = extract.apply(StatementExecutor.ENTITY_UPDATE_TIMESTAMP_COLUMN_NAME);

                return Optional.of(Payload.map(
                        getIdentifierProvider().getName(), getCoercer().coerce(idInRecord, getIdentifierProvider().getType()),
                        StatementExecutor.ENTITY_TYPE_MAP_KEY, getCoercer().coerce(entityTypeInRecord, String.class),
                        StatementExecutor.ENTITY_VERSION_MAP_KEY, getCoercer().coerce(entityVersionInRecord, Integer.class),
                        StatementExecutor.ENTITY_CREATE_USERNAME_MAP_KEY, getCoercer().coerce(entityCreateUsernameInRecord, String.class),
                        StatementExecutor.ENTITY_CREATE_USER_ID_MAP_KEY, getCoercer().coerce(entityCreateUserIdInRecord, getIdentifierProvider().getType()),
                        StatementExecutor.ENTITY_CREATE_TIMESTAMP_MAP_KEY, getCoercer().coerce(entityCreateTimestampInRecord, OffsetDateTime.class),
                        StatementExecutor.ENTITY_UPDATE_USERNAME_MAP_KEY, getCoercer().coerce(entityUpdateUsernameInRecord, String.class),
                        StatementExecutor.ENTITY_UPDATE_USER_ID_MAP_KEY, getCoercer().coerce(entityUpdateUserIdInRecord, getIdentifierProvider().getType()),
                        StatementExecutor.ENTITY_UPDATE_TIMESTAMP_MAP_KEY, getCoercer().coerce(entityUpdateTimestampInRecord, OffsetDateTime.class)
                ));
            } finally {
                rdbmsBuilder.getConstantFields().remove();
            }
        } else {
            rdbmsBuilder.getConstantFields().remove();
            return Optional.empty();
        }
    }


    /**
     * Execute a logical query on a given JDBC template and return result set.
     *
     * @param jdbcTemplate             JDBC template
     * @param mappedTransferObjectType transfer object type of results
     * @param ids                      instance IDs
     * @param queryCustomizer          query customizer
     * @return result set (list of records) that can has embedded result sets
     */
    public Collection<Payload> executeSelect(final NamedParameterJdbcTemplate jdbcTemplate, final EClass mappedTransferObjectType, Collection<ID> ids, final DAO.QueryCustomizer<ID> queryCustomizer) {
        try (MetricsCancelToken ct = metricsCollector.start(METRICS_SELECT_PREPARE)) {
            rdbmsBuilder.getConstantFields().set(new HashMap<>());
            final Select _select = queryFactory.getQuery(mappedTransferObjectType).get();

            final Select select;
            if (queryCustomizer != null && (queryCustomizer.getFilter() != null || queryCustomizer.getOrderByList() != null && !queryCustomizer.getOrderByList().isEmpty() || queryCustomizer.getSeek() != null)) {
                select = clone(_select);
            } else {
                select = _select;
            }
            final SubSelect query = newSubSelectBuilder()
                    .withSelect(select)
                    .withAlias("")
                    .build();

            applyQueryCustomizer(query, queryCustomizer);
            if (queryCustomizer != null && queryCustomizer.getInstanceIds() != null && ids != null) {
                ids.addAll(queryCustomizer.getInstanceIds());
            } else if (queryCustomizer != null && queryCustomizer.getInstanceIds() != null && ids == null) {
                ids = queryCustomizer.getInstanceIds();
            }

            final EMap<Target, Map<ID, Payload>> result = runQuery(jdbcTemplate, query, ids, null, ECollections.emptyEList(), queryCustomizer != null ? queryCustomizer.getSeek() : null, queryCustomizer != null ? queryCustomizer.isWithoutFeatures() : false, queryCustomizer != null ? queryCustomizer.getMask() : null, queryCustomizer != null ? queryCustomizer.getParameters() : null, true);

            final Collection<Payload> ret = result.get(select.getMainTarget()).values();
            if (queryCustomizer != null && (queryCustomizer.getOrderByList() != null && !queryCustomizer.getOrderByList().isEmpty() || queryCustomizer.getSeek() != null)) {
                final List<Payload> list = new ArrayList<>(ret);
                if (queryCustomizer.getSeek() != null && queryCustomizer.getSeek().isReverse()) {
                    Collections.reverse(list);
                }
                return list;
            } else {
                return new HashSet<>(ret);
            }
        } finally {
            rdbmsBuilder.getConstantFields().remove();
        }
    }

    public Payload executeSelect(final NamedParameterJdbcTemplate jdbcTemplate, final EAttribute attribute, final Map<String, Object> parameters) {
        try (MetricsCancelToken ct = metricsCollector.start(METRICS_SELECT_PREPARE)) {
            rdbmsBuilder.getConstantFields().set(new HashMap<>());
            final SubSelect subSelect = queryFactory.getDataQuery(attribute).orElseThrow(() -> new IllegalStateException("Query for static data not prepared yet"));

            final EMap<Target, Map<ID, Payload>> results = runQuery(jdbcTemplate, subSelect, null, null, ECollections.emptyEList(), null, false, Collections.singletonMap(attribute.getName(), true), parameters, true);

            final Collection<Payload> resultSet = results.get(subSelect.getSelect().getMainTarget()).values();
            checkArgument(resultSet != null && resultSet.size() == 1, "Invalid result set");

            final Payload result = resultSet.iterator().next();
            return Payload.asPayload(Collections.singletonMap(attribute.getName(), result.get(attribute.getName())));
        } finally {
            rdbmsBuilder.getConstantFields().remove();
        }
    }

    /**
     * Execute a logical query on a given JDBC template and return result set.
     *
     * @param jdbcTemplate    JDBC template
     * @param reference       reference that is used by navigation from parents to get results
     * @param ids             instance IDs (if reference is not set) or parent IDs (if reference is set)
     * @param queryCustomizer query customizer
     * @return result set (list of records) that can has embedded result sets
     */
    public Collection<Payload> executeSelect(final NamedParameterJdbcTemplate jdbcTemplate, final EReference reference, final Collection<ID> ids, final DAO.QueryCustomizer<ID> queryCustomizer) {
        try (MetricsCancelToken ct = metricsCollector.start(METRICS_SELECT_PREPARE)) {
            rdbmsBuilder.getConstantFields().set(new HashMap<>());
            final EClass referenceHolder = reference.getEContainingClass();

            final SubSelect _query;
            if (!AsmUtils.isEntityType(referenceHolder) && !asmUtils.isMappedTransferObjectType(referenceHolder)) {
                _query = queryFactory.getNavigation(reference).get();
            } else {
                final Select base = queryFactory.getQuery(reference.getEContainingClass()).get();

                final Optional<SubSelect> subSelect = base.getSubSelects().stream()
                        .filter(s -> s.getTransferRelation() != null && AsmUtils.equals(s.getTransferRelation(), reference))
                        .findAny();

                checkArgument(subSelect.isPresent(), "Subselect must be prepared by query factory");
                _query = subSelect.get();
            }

            final SubSelect query;
            if (queryCustomizer != null && (queryCustomizer.getFilter() != null || queryCustomizer.getOrderByList() != null && !queryCustomizer.getOrderByList().isEmpty() || queryCustomizer.getSeek() != null)) {
                if (_query.eContainer() != null) {
                    Node _container = (Node) clone(_query.eContainer());
                    query = _container.getSubSelects().stream()
                            .filter(ss -> Objects.equals(ss.getAlias(), _query.getAlias()))
                            .findAny()
                            .orElseThrow(() -> new IllegalStateException("SubSelect not cloned correctly"));
                } else {
                    query = clone(_query);
                }
                final Select select = clone(_query.getSelect());
                query.setSelect(select);
            } else {
                query = _query;
            }

            applyQueryCustomizer(query, queryCustomizer);

            Collection<ID> instanceIds;
            final Collection<ID> parentIds;
            final boolean useIdsAsParents = reference != null && !(query.getNavigationJoins().isEmpty() && !queryFactory.isStaticReference(reference));
            if (useIdsAsParents) {
                instanceIds = null;
                parentIds = ids;
            } else {
                instanceIds = ids;
                parentIds = Collections.emptySet();
            }
            if (queryCustomizer != null && queryCustomizer.getInstanceIds() != null && instanceIds != null) {
                instanceIds.addAll(queryCustomizer.getInstanceIds());
            } else if (queryCustomizer != null && queryCustomizer.getInstanceIds() != null && instanceIds == null) {
                instanceIds = queryCustomizer.getInstanceIds();
            }

            final EMap<Target, Map<ID, Payload>> subQueryResults = runQuery(jdbcTemplate, query, instanceIds, parentIds, reference != null ? ECollections.singletonEList(reference) : ECollections.emptyEList(), queryCustomizer != null ? queryCustomizer.getSeek() : null, queryCustomizer != null ? queryCustomizer.isWithoutFeatures() : false, queryCustomizer != null ? queryCustomizer.getMask() : null, queryCustomizer != null ? queryCustomizer.getParameters() : null, true);

            final Target subQueryTarget = query.getSelect().getMainTarget();

            final Collection<Payload> results = queryFactory.isOrdered(reference) || queryCustomizer != null && (queryCustomizer.getOrderByList() != null && !queryCustomizer.getOrderByList().isEmpty() || queryCustomizer.getSeek() != null) ? new ArrayList<>() : new HashSet<>();

            final String subParentKey = RdbmsAliasUtil.getParentIdColumnAlias(query.getContainer());
            subQueryResults.get(subQueryTarget).values().forEach(subQueryRecord -> {
                if (subParentKey != null) {
                    if (!subQueryRecord.containsKey(subParentKey)) { // container record of subquery record found
                        log.trace("Unknown parent ID ({})", subParentKey);
                    }
                    subQueryRecord.remove(subParentKey); // remove parent key from subquery record because it will be added as nested list
                }
                results.add(subQueryRecord);
            });

            if (queryCustomizer != null && queryCustomizer.getSeek() != null && queryCustomizer.getSeek().isReverse()) {
                Collections.reverse((List<Payload>) results);
            }
            return results;
        } finally {
            rdbmsBuilder.getConstantFields().remove();
        }
    }

    private void applyQueryCustomizer(final SubSelect query, final DAO.QueryCustomizer<ID> queryCustomizer) {
        final EClass mainTarget = query.getSelect().getMainTarget().getType();

        if (queryCustomizer != null && queryCustomizer.getFilter() != null) {
            final Filter filter = createFilter(mainTarget, query.getSelect(), queryCustomizer.getFilter());
            query.getSelect().getFilters().add(filter);
        }
        final EMap<EAttribute, Feature> mainFeatures = ECollections.asEMap(query.getSelect().getFeatures().stream()
                .filter(f -> (f instanceof Attribute || f instanceof Function || f instanceof SubSelectFeature) && f.getTargetMappings().stream().anyMatch(tm -> AsmUtils.equals(tm.getTarget(), query.getSelect().getMainTarget())))
                .collect(Collectors.toMap(f -> f.getTargetMappings().stream().filter(tm -> AsmUtils.equals(tm.getTarget(), query.getSelect().getMainTarget())).findAny().get().getTargetAttribute(), f -> f)));
        final boolean reverse = queryCustomizer != null && queryCustomizer.getSeek() != null ? queryCustomizer.getSeek().isReverse() : false;
        if (queryCustomizer != null && queryCustomizer.getOrderByList() != null && !queryCustomizer.getOrderByList().isEmpty()) {
            query.getNavigationJoins().forEach(j -> j.getOrderBys().clear());
            query.getSelect().getOrderBys().clear();
            query.getOrderBys().clear();
            query.getSelect().getOrderBys().addAll(queryCustomizer.getOrderByList().stream()
                    .map(o -> newOrderByBuilder().withFeature(mainFeatures.get(o.getAttribute())).withDescending(reverse ? !o.isDescending() : o.isDescending()).build())
                    .collect(Collectors.toList()));
        }
        if (queryCustomizer != null && queryCustomizer.getSeek() != null) {
            query.setLimit(queryCustomizer.getSeek().getLimit());
        }
    }

    private synchronized <T extends EObject> T clone(final T original) {
        final EcoreUtil.Copier copier = new EcoreUtil.Copier(true, true);
        final T result = (T) copier.copy(original);
        copier.copyReferences();

        return result;
    }

    private Filter createFilter(final EClass type, final Node node, final String filterExpressionString) {
        // TODO - use request ID in URI
        final ExpressionModelResourceSupport expressionModelResourceSupport = ExpressionModelResourceSupport.expressionModelResourceSupportBuilder()
                .uri(URI.createURI("expression:filter-" + UUID.randomUUID()))
                .build();
        final JqlExpressionBuilder<EClassifier, EDataType, EEnum, EClass, EAttribute, EReference, EClass, EAttribute, EReference, EClassifier, Measure, Unit> jqlExpressionBuilder = new JqlExpressionBuilder<>(queryFactory.getModelAdapter(), expressionModelResourceSupport.getResource());
        final TypeName typeName = jqlExpressionBuilder
                .buildTypeName(type)
                .orElseThrow(() -> new IllegalStateException("Invalid type name"));

        final JqlExpressionBuildingContext context = new JqlExpressionBuildingContext();
        final Instance _this = newInstanceBuilder()
                .withName("this")
                .withElementName(typeName)
                .build();
        context.pushVariable(_this);

        JqlExpression filterExpressionJql;
        synchronized (this) {
            filterExpressionJql = new JqlParser().parseString(filterExpressionString);
        }
        final LogicalExpression filterExpression = (LogicalExpression) jqlExpressionBuilder.createExpression(filterExpressionJql, context);

        final LogicalExpression translatedFilterExpression = (LogicalExpression) translator.apply(filterExpression);

        if (log.isDebugEnabled()) {
            log.debug("Translated transfer object filter to entity filter:\n    {}\n => {}", filterExpression, translatedFilterExpression);
        }

        final Feature filter = queryFactory.dataExpressionToFeature(translatedFilterExpression, Context.builder()
                .node(node)
                .sourceCounter(queryFactory.getNextSourceIndex())
                .targetCounter(queryFactory.getNextTargetIndex())
                .variables(Collections.singletonMap("self", node))
                .build(), null, null);
        return newFilterBuilder()
                .withAlias("_f" + node.getAlias())
                .withFeature(filter)
                .build();
    }

    /**
     * Run logical logical subquery and return result set.
     * <p>
     * Result set is a map of which key is the target definition and value is map of records. Names may be different
     * from ASM model in result set because most of the RDBMS implementations are case-insensitive, the problem is
     * handled by this method.
     *
     * @param jdbcTemplate    JDBC template
     * @param query           logical (sub)query
     * @param instanceIds     instance IDs to select
     * @param parentIds       set of parent IDs that the subquery records are embedded in (as list items)
     * @param referenceChain  list of references
     * @param withoutFeatures load data without features (i.e. for validating requests)
     * @param mask            mask containing features to get
     * @param queryParameters query parameters
     * @param skipParents     skip parent IDs from result
     * @return result set
     */
    private EMap<Target, Map<ID, Payload>> runQuery(final NamedParameterJdbcTemplate jdbcTemplate, final SubSelect query, final Collection<ID> instanceIds, final Collection<ID> parentIds, final EList<EReference> referenceChain, final DAO.Seek seek, final boolean withoutFeatures, final Map<String, Object> mask, final Map<String, Object> queryParameters, final boolean skipParents) {
        // map of sources, key is ID column alias, value is the source object (including SELECT and all JOINs)
        final Map<String, Node> sources = new ConcurrentHashMap<>(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(j -> RdbmsAliasUtil.getIdColumnAlias(j), j -> j)));
        sources.put(RdbmsAliasUtil.getIdColumnAlias(query.getSelect()), query.getSelect());

        final Map<String, Node> types = new ConcurrentHashMap<>(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(j -> RdbmsAliasUtil.getTypeColumnAlias(j), j -> j)));
        types.put(RdbmsAliasUtil.getTypeColumnAlias(query.getSelect()), query.getSelect());

        final Map<String, Node> versions = new ConcurrentHashMap<>(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(j -> RdbmsAliasUtil.getVersionColumnAlias(j), j -> j)));
        versions.put(RdbmsAliasUtil.getVersionColumnAlias(query.getSelect()), query.getSelect());

        final Map<String, Node> createUsernames = new ConcurrentHashMap<>(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(j -> RdbmsAliasUtil.getCreateUsernameColumnAlias(j), j -> j)));
        createUsernames.put(RdbmsAliasUtil.getCreateUsernameColumnAlias(query.getSelect()), query.getSelect());

        final Map<String, Node> createUserIds = new ConcurrentHashMap<>(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(j -> RdbmsAliasUtil.getCreateUserIdColumnAlias(j), j -> j)));
        createUserIds.put(RdbmsAliasUtil.getCreateUserIdColumnAlias(query.getSelect()), query.getSelect());

        final Map<String, Node> createTimestamps = new ConcurrentHashMap<>(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(j -> RdbmsAliasUtil.getCreateTimestampColumnAlias(j), j -> j)));
        createTimestamps.put(RdbmsAliasUtil.getCreateTimestampColumnAlias(query.getSelect()), query.getSelect());

        final Map<String, Node> updateUsernames = new ConcurrentHashMap<>(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(j -> RdbmsAliasUtil.getUpdateUsernameColumnAlias(j), j -> j)));
        updateUsernames.put(RdbmsAliasUtil.getUpdateUsernameColumnAlias(query.getSelect()), query.getSelect());

        final Map<String, Node> updateUserIds = new ConcurrentHashMap<>(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(j -> RdbmsAliasUtil.getUpdateUserIdColumnAlias(j), j -> j)));
        updateUserIds.put(RdbmsAliasUtil.getUpdateUserIdColumnAlias(query.getSelect()), query.getSelect());

        final Map<String, Node> updateTimestamps = new ConcurrentHashMap<>(query.getSelect().getAllJoins().stream().collect(Collectors.toMap(j -> RdbmsAliasUtil.getUpdateTimestampColumnAlias(j), j -> j)));
        updateTimestamps.put(RdbmsAliasUtil.getUpdateTimestampColumnAlias(query.getSelect()), query.getSelect());

        // the map that will store results
        final EMap<Target, Map<ID, Payload>> results = ECollections.asEMap(
                query.getSelect().getTargets().stream()
                        .collect(Collectors.toMap(target -> target, target -> new LinkedHashMap<>())));

        // get JDBC result set and process the records
        if (log.isDebugEnabled()) {
            log.debug("Running query: {}", referenceChain.stream().map(r -> r.getName()).collect(Collectors.joining(".")));
        }
        if (log.isTraceEnabled()) {
            log.trace(" - logical query:\n{}", query.getSelect());
        }
        log.debug("Instance IDs: {}", instanceIds);
        log.debug("Parent IDs: {}", parentIds);

        final RdbmsResultSet resultSetHandler = new RdbmsResultSet(query, instanceIds != null, parentIds != null ? query : null, rdbmsBuilder, seek, withoutFeatures, mask, queryParameters, skipParents);

        final List<Chunk<ID>> chunks = new ArrayList<>();
        if (parentIds != null) {
            final List<List<ID>> _parentIds = parentIds.isEmpty() ? Collections.singletonList(Collections.emptyList()) : Lists.partition(new ArrayList<>(parentIds), chunkSize);
            _parentIds.forEach(p -> {
                if (instanceIds != null) {
                    final List<List<ID>> _instanceIds = instanceIds.isEmpty() ? Collections.singletonList(Collections.emptyList()) : Lists.partition(new ArrayList<>(instanceIds), chunkSize);
                    _instanceIds.forEach(i ->
                            chunks.add(Chunk.<ID>builder().parentIds(p).instanceIds(i).build()));
                } else {
                    chunks.add(Chunk.<ID>builder().parentIds(p).build());
                }
            });
        } else if (instanceIds != null) {
            final List<List<ID>> _instanceIds = instanceIds.isEmpty() ? Collections.singletonList(Collections.emptyList()) : Lists.partition(new ArrayList<>(instanceIds), chunkSize);
            _instanceIds.forEach(i ->
                    chunks.add(Chunk.<ID>builder().instanceIds(i).build()));
        } else {
            chunks.add(Chunk.<ID>builder().build());
        }
        chunks.forEach(chunk -> {
            log.debug("Running chunk: {}", chunk);
            final MapSqlParameterSource sqlParameters = new MapSqlParameterSource();
            if (chunk.parentIds != null) {
                sqlParameters.addValue(RdbmsAliasUtil.getParentIdsKey(query.getSelect()), chunk.parentIds.stream().map(id -> getCoercer().coerce(id, getRdbmsParameterMapper().getIdClassName())).collect(Collectors.toList()));
            }
            if (chunk.instanceIds != null) {
                sqlParameters.addValue(RdbmsAliasUtil.getInstanceIdsKey(query.getSelect()), chunk.instanceIds.stream().map(id -> getCoercer().coerce(id, getRdbmsParameterMapper().getIdClassName())).collect(Collectors.toList()));
            }

            // key used to identify parent instance in subselects
            final String parentKey = RdbmsAliasUtil.getParentIdColumnAlias(query.getContainer());

            final String sql = resultSetHandler.toSql("", false, getCoercer(), sqlParameters, ECollections.emptyEMap());
            log.debug("SQL:\n--------------------------------------------------------------------------------\n{}", sql);
            log.debug("Parameters: {}", sqlParameters.getValues());

            List<Map<String, Object>> resultSet;
            try (MetricsCancelToken ct = metricsCollector.start(METRICS_SELECT_QUERY)) {
                resultSet = jdbcTemplate.queryForList(sql, sqlParameters);
            }

            try (MetricsCancelToken ct = metricsCollector.start(METRICS_SELECT_PROCESSING)) {
                resultSet.forEach(record -> {
                    log.debug("Processing record:\n{}", record);

                    // map containing records by target (extracted from current JDBC record)
                    final EMap<Target, Payload> recordsByTarget = ECollections.asEMap(new HashMap<>());
                    // IDs of current JDBC record by targets
                    final EMap<Target, ID> idsByTarget = ECollections.asEMap(new HashMap<>());
                    // Unset (NULL) target references in database
                    final EList<Target> nullTargets = ECollections.newBasicEList();

                    query.getSelect().getTargets().stream()
                            .forEach(target -> recordsByTarget.put(target, Payload.asPayload(new ConcurrentHashMap<>())));
                    if (chunk.parentIds != null && parentKey != null) {
                        recordsByTarget.get(query.getSelect().getMainTarget()).put(parentKey, new HashSet<>());
                    }

                    // process fields of record
                    record.entrySet().stream().forEach(field -> {
                        log.trace("  - key: {}", field.getKey());
                        log.trace("  - value: {} ({})", field.getValue(), field.getValue() != null ? field.getValue().getClass().getName() : "-");

                        final Optional<Map.Entry<String, Node>> idSource = sources.entrySet().stream().filter(s -> s.getKey().equalsIgnoreCase(field.getKey())).findAny();
                        final Optional<Map.Entry<String, Node>> typeSource = types.entrySet().stream().filter(t -> t.getKey().equalsIgnoreCase(field.getKey())).findAny();
                        final Optional<Map.Entry<String, Node>> versionSource = versions.entrySet().stream().filter(t -> t.getKey().equalsIgnoreCase(field.getKey())).findAny();
                        final Optional<Map.Entry<String, Node>> createUsernameSource = createUsernames.entrySet().stream().filter(t -> t.getKey().equalsIgnoreCase(field.getKey())).findAny();
                        final Optional<Map.Entry<String, Node>> createUserIdSource = createUserIds.entrySet().stream().filter(t -> t.getKey().equalsIgnoreCase(field.getKey())).findAny();
                        final Optional<Map.Entry<String, Node>> createTimestampSource = createTimestamps.entrySet().stream().filter(t -> t.getKey().equalsIgnoreCase(field.getKey())).findAny();
                        final Optional<Map.Entry<String, Node>> updateUsernameSource = updateUsernames.entrySet().stream().filter(t -> t.getKey().equalsIgnoreCase(field.getKey())).findAny();
                        final Optional<Map.Entry<String, Node>> updateUserIdSource = updateUserIds.entrySet().stream().filter(t -> t.getKey().equalsIgnoreCase(field.getKey())).findAny();
                        final Optional<Map.Entry<String, Node>> updateTimestampSource = updateTimestamps.entrySet().stream().filter(t -> t.getKey().equalsIgnoreCase(field.getKey())).findAny();

                        final Map<String, Optional<Map.Entry<String, Node>>> metaSources = ImmutableMap.<String, Optional<Map.Entry<String, Node>>>builder()
                                .put(ENTITY_TYPE_MAP_KEY, typeSource)
                                .put(ENTITY_VERSION_MAP_KEY, versionSource)
                                .put(ENTITY_CREATE_USERNAME_MAP_KEY, createUsernameSource)
                                .put(ENTITY_CREATE_USER_ID_MAP_KEY, createUserIdSource)
                                .put(ENTITY_CREATE_TIMESTAMP_MAP_KEY, createTimestampSource)
                                .put(ENTITY_UPDATE_USERNAME_MAP_KEY, updateUsernameSource)
                                .put(ENTITY_UPDATE_USER_ID_MAP_KEY, updateUserIdSource)
                                .put(ENTITY_UPDATE_TIMESTAMP_MAP_KEY, updateTimestampSource)
                                .build();

                        if (idSource.isPresent()) {
                            log.trace("    - id source: {}", idSource.get().getValue().getAlias());

                            final EList<Target> foundTargets = ECollections.asEList(query.getSelect().getTargets().stream()
                                    .filter(t -> Objects.equals(t.getNode(), idSource.get().getValue()))
                                    .collect(Collectors.toList()));

                            if (!foundTargets.isEmpty()) {
                                foundTargets.forEach(target -> {
                                    log.trace("    - id target: {}", target);
                                    final ID id = getCoercer().coerce(field.getValue(), getIdentifierProvider().getType());
                                    recordsByTarget.get(target).put(getIdentifierProvider().getName(), id);

                                    idsByTarget.put(target, id);
                                    if (id == null) {
                                        nullTargets.add(target);
                                    }
                                });
                            } else {
                                log.trace("    - no target of ID, source alias: {}, column: {}", idSource.get().getValue().getAlias(), idSource.get().getKey());
                            }
                        } else if (metaSources.entrySet().stream().anyMatch(e -> e.getValue().isPresent())) {
                            metaSources.entrySet().stream()
                                    .filter(e -> e.getValue().isPresent())
                                    .forEach(e -> {
                                        log.trace("    - meta source: {}", e.getValue().get().getValue().getAlias());

                                        final EList<Target> foundTargets = ECollections.asEList(query.getSelect().getTargets().stream()
                                                .filter(t -> Objects.equals(t.getNode(), e.getValue().get().getValue()))
                                                .collect(Collectors.toList()));

                                        if (!foundTargets.isEmpty()) {
                                            foundTargets.forEach(target -> {
                                                log.trace("    - meta target: {}", target);

                                                final Object value;
                                                if (ENTITY_CREATE_TIMESTAMP_MAP_KEY.equals(e.getKey()) || ENTITY_UPDATE_TIMESTAMP_MAP_KEY.equals(e.getKey())) {
                                                    value = getCoercer().coerce(field.getValue(), OffsetDateTime.class);
                                                } else if (ENTITY_CREATE_USER_ID_MAP_KEY.equals(e.getKey()) || ENTITY_UPDATE_USER_ID_MAP_KEY.equals(e.getKey())) {
                                                    value = getCoercer().coerce(field.getValue(), getIdentifierProvider().getType());
                                                } else {
                                                    value = field.getValue();
                                                }

                                                recordsByTarget.get(target).put(e.getKey(), value);
                                            });
                                        } else {
                                            log.trace("    - no target of type, source alias: {}, column: {}", e.getValue().get().getValue().getAlias(), e.getValue().get().getKey());
                                        }
                                    });
                        } else if (chunk.parentIds != null && parentKey != null && parentKey.equalsIgnoreCase(field.getKey())) {
                            final ID id = getCoercer().coerce(field.getValue(), getIdentifierProvider().getType());
                            log.trace("    - parent key: {}", parentKey);
                            recordsByTarget.get(query.getSelect().getMainTarget()).getAs(Collection.class, parentKey).add(id);
                        } else {
                            final Set<Target> foundTargets = new HashSet<>();
                            query.getSelect().getTargets().stream().forEach(target -> {
                                log.trace("   - target: {}", target);
                                // get attribute the field is matching to
                                final Optional<FeatureTargetMapping> featureTargetMapping = query.getSelect().getFeatures().stream()
                                        .flatMap(f -> f.getTargetMappings().stream()
                                                .filter(tm -> AsmUtils.equals(tm.getTarget(), target) && tm.getTargetAttribute() != null && RdbmsAliasUtil.getTargetColumnAlias(target, RdbmsMapper.getAttributeOrFeatureName(tm.getTargetAttribute(), null)).equalsIgnoreCase(field.getKey())))
                                        .findAny();
                                if (featureTargetMapping.isPresent()) { // attribute found in the current target
                                    final Feature feature = (Feature) featureTargetMapping.get().eContainer();

                                    log.trace("    - attribute: {}", feature);
                                    final EAttribute attribute = featureTargetMapping.get().getTargetAttribute();
                                    final Optional<String> customTypeName = AsmUtils.getExtensionAnnotationCustomValue(attribute, "constraints", "customType", false);

                                    final Object convertedValue;
                                    if (field.getValue() != null) {
                                        final String className;
                                        Object value = field.getValue();
                                        if (customTypeName.isPresent()) {
                                            log.debug("Using custom type: {}", customTypeName.get());
                                            final Optional<EDataType> dataType = asmUtils.resolve(customTypeName.get()).filter(type -> type instanceof EDataType).map(type -> (EDataType) type);
                                            checkArgument(dataType.isPresent(), "Unknown data type: " + customTypeName.get());
                                            className = dataTypeManager.getCustomTypeName(dataType.get()).orElse(null);
                                            checkArgument(className != null, "Unregistered custom type: " + dataType.get().getInstanceClassName());
                                        } else if (attribute.getEAttributeType() instanceof EEnum) {
                                            className = Integer.class.getName();
                                        } else if (AsmUtils.isByteArray(attribute.getEAttributeType())) {
                                            className = FileType.class.getName();
                                        } else {
                                            className = attribute.getEAttributeType().getInstanceClassName();
                                        }
                                        convertedValue = getCoercer().coerce(value, className);
                                    } else {
                                        convertedValue = null;
                                    }

                                    log.trace("    - converted value: {}", convertedValue);
                                    recordsByTarget.get(featureTargetMapping.get().getTarget()).put(featureTargetMapping.get().getTargetAttribute().getName(), convertedValue);
                                    foundTargets.add(featureTargetMapping.get().getTarget());
                                }
                            });
                            if (foundTargets.isEmpty()) {
                                log.debug("No target found for {}, value: {}", field.getKey(), field.getValue());
                            }
                        }
                    });

                    // replace payload with null if ID of target is NULL
                    nullTargets.forEach(target -> recordsByTarget.put(target, null));

                    query.getSelect().getTargets().stream()
                            .filter(target -> recordsByTarget.get(target) != null)
                            .forEach(target -> {
                                if (log.isTraceEnabled()) {
                                    log.trace("  - setting containments of {}", target);
                                }

                                // set containments that are selected in single (joined) query
                                target.getReferencedTargets().stream()
                                        .filter(rt -> isEmbedded(rt.getReference()))
                                        .filter(c -> !c.getReference().isMany() && !AsmUtils.equals(query.getSelect().getMainTarget(), c.getTarget()))
                                        .filter(c -> !withoutFeatures && (mask == null || mask.containsKey(c.getReference().getName())))
                                        .forEach(c -> {
                                            if (log.isTraceEnabled()) {
                                                log.trace("    - add: {} AS {}", c.getTarget(), c.getReference().getName());
                                            }
                                            final Map<String, Object> containment = recordsByTarget.get(c.getTarget());
                                            recordsByTarget.get(target).put(c.getReference().getName(), containment);
                                        });

                                // set containments that will be selected in separate query (multiple relationship or aggregation) to empty list
                                target.getReferencedTargets().stream()
                                        .filter(r -> isEmbedded(r.getReference()))
                                        .filter(c -> c.getReference().isMany())
                                        .filter(c -> !withoutFeatures && (mask == null || mask.containsKey(c.getReference().getName())))
                                        .forEach(c -> recordsByTarget.get(target).put(c.getReference().getName(), queryFactory.isOrdered(c.getReference()) ? new ArrayList<>() : new HashSet<>()));

                                // set all results of targets in result
                                if (idsByTarget.containsKey(target) && idsByTarget.get(target) != null) {
                                    final ID targetID = idsByTarget.get(target);

                                    if (parentKey != null && results.get(target).containsKey(targetID) && results.get(target).get(targetID).get(parentKey) != null) {
                                        log.debug("Record already added, add new parentId only");
                                        final Collection<ID> currentParentIds = results.get(target).get(targetID).getAs(Collection.class, parentKey);
                                        log.trace("Current  parent IDs: {}", currentParentIds);
                                        final Collection<ID> newParentIds = recordsByTarget.get(target).getAs(Collection.class, parentKey);
                                        log.trace("New parent IDs: {}", newParentIds);
                                        currentParentIds.addAll(newParentIds);
                                    } else {
                                        results.get(target).put(idsByTarget.get(target), recordsByTarget.get(target));
                                    }
                                } else if (idsByTarget.isEmpty() && AsmUtils.equals(query.getSelect().getMainTarget(), target)) {
                                    final ID tmpId = getCoercer().coerce(UUID.randomUUID(), getRdbmsParameterMapper().getIdClassName());
                                    results.get(target).put(tmpId, recordsByTarget.get(target));
                                }
                            });

                    log.trace("Records by target:\n{}", recordsByTarget);
                });

                if (!withoutFeatures) {
                    final EMap<EList<EReference>, Target> allTargetPaths = getAllTargetPaths(query.getSelect().getMainTarget());
                    allTargetPaths.entrySet().stream()
                            .filter(tp -> tp.getKey().stream().allMatch(r -> isEmbedded(r) && !r.isMany()))
                            .forEach(tp ->
                                    tp.getValue().getNode().getSubSelects().stream()
                                            .filter(subSelect -> subSelect.getTransferRelation() != null && isEmbedded(subSelect.getTransferRelation()) && !subSelect.isExcluding())
                                            .filter(subSelect -> mask == null || mask.containsKey(subSelect.getTransferRelation().getName()))
                                            .forEach(subSelect -> runSubQuery(jdbcTemplate, query, subSelect, ECollections.asEList(ImmutableList.<EReference>builder().addAll(referenceChain).addAll(tp.getKey()).build()), results, mask != null ? (Map<String, Object>) mask.get(subSelect.getTransferRelation().getName()) : null, queryParameters)));
                }

                log.trace("Query results:\n{}", results);
            }
        });

        return results;
    }

    private void runSubQuery(final NamedParameterJdbcTemplate jdbcTemplate, final SubSelect query, final SubSelect subSelect, final EList<EReference> referenceChain, final EMap<Target, Map<ID, Payload>> results, final Map<String, Object> mask, final Map<String, Object> queryParameters) {
        checkArgument(subSelect.getTransferRelation() != null, "SubSelect must have transfer relation");

        final EList<EReference> newReferenceChain = new BasicEList<>();
        newReferenceChain.addAll(referenceChain);
        newReferenceChain.add(subSelect.getTransferRelation());

        log.trace("Preparing subselect: {} joining {}", new Object[]{subSelect.getTransferRelation().getName(), subSelect.getNavigationJoins().stream().map(j -> (j instanceof ReferencedJoin) ? ((ReferencedJoin) j).getReference().getName() : "?").collect(Collectors.joining(", "))});

        Optional<Target> subTarget = query.getSelect().getTargets().stream().filter(t -> AsmUtils.equals(t.getNode(), subSelect.getContainer())).findAny();
        if (subTarget.isPresent()) {
            final Set<ID> ids = results.get(subTarget.get()).keySet();
            if (!ids.isEmpty()) {
                if (ids.size() == 1 ||
                        (AsmUtils.equals(subSelect.getBase(), query.getSelect()) || query.getSelect().getAllJoins().contains(subSelect.getBase())) && subSelect.getLimit() == null) {
                    executeSubQuery(jdbcTemplate, query, subSelect, newReferenceChain, results, ids, mask, queryParameters);
                } else {
                    ids.forEach(id -> executeSubQuery(jdbcTemplate, query, subSelect, newReferenceChain, results, Collections.singleton(id), mask, queryParameters));
                }
            }
        } else {
            throw new UnsupportedOperationException("Not supported yet");
        }
    }

    private void executeSubQuery(final NamedParameterJdbcTemplate jdbcTemplate, final SubSelect query, final SubSelect subSelect, final EList<EReference> newReferenceChain, final EMap<Target, Map<ID, Payload>> results, final Collection<ID> ids, final Map<String, Object> mask, final Map<String, Object> queryParameters) {
        log.trace("  IDs: {}", ids);

        // map storing subquery results, it will be filled by recursive call
        final EMap<Target, Map<ID, Payload>> subQueryResults = runQuery(jdbcTemplate, subSelect, null, ids, newReferenceChain, null, false, mask, queryParameters, false);

        if (log.isDebugEnabled()) {
            log.debug("Processing subquery results: {}", newReferenceChain.stream().map(r -> r.getName()).collect(Collectors.joining(".")));
        }

        final Target subQueryTarget = subSelect.getSelect().getMainTarget();
        final String subParentKey = RdbmsAliasUtil.getParentIdColumnAlias(subSelect.getContainer());
        log.trace("  - subquery target: {}", subQueryTarget);
        log.trace("  - parent key: {}", subParentKey);

        subQueryResults.get(subQueryTarget).values().forEach(subQueryRecord -> {
            final Collection<ID> parentIds = (Collection<ID>) subQueryRecord.get(subParentKey);
            log.trace("    - parent IDs: {}", parentIds);

            checkArgument(parentIds != null, "Unknown parent ID");
            final Collection<Payload> containers = new ArrayList<>();
            if (parentIds.isEmpty()) {
                checkArgument(ids.size() == 1, "Parent IDs must be single");
                final ID id = ids.iterator().next();
                log.trace("      - (parent) ID: {}", id);
                final Target target = query.getSelect().getMainTarget();
                log.trace("        - target: {}", target);
                checkArgument(results.containsKey(target), "No target found in results");

                final Map<ID, Payload> targetResult = results.get(target);
                if (targetResult.containsKey(id)) {
                    containers.add(targetResult.get(id));
                } else {
                    if (subSelect.getPartner() instanceof SubSelectJoin) {
                        log.debug("No (parent) ID found in container but object selector used, adding all targets");
                        containers.addAll(targetResult.values());
                    } else {
                        log.debug("No (parent) ID found in container result");
                    }
                }
            } else {
                parentIds.forEach(parentId -> {
                            log.trace("      - parent ID: {}", parentId);
                            query.getSelect().getTargets().forEach(target -> {
                                log.trace("        - target: {}", target);
                                checkArgument(results.containsKey(target), "No target found in results");

                                final Map<ID, Payload> targetResult = results.get(target);
                                if (targetResult.containsKey(parentId)) {
                                    containers.add(targetResult.get(parentId));
                                } else {
                                    if (subSelect.getPartner() instanceof SubSelectJoin) {
                                        log.debug("No parent ID found in container but object selector used, adding all targets");
                                        containers.addAll(targetResult.values());
                                    } else {
                                        log.debug("No parent ID found in container result");
                                    }
                                }
                            });
                        }
                );
            }
            containers.forEach(container -> {
                final Object containment = container.get(subSelect.getTransferRelation().getName());
                final boolean initialized = containment != null; // initialized if containment key already present and value is not null
                final boolean many = subSelect.getTransferRelation().isMany();

                if (initialized && many) {
                    ((Collection) containment).add(subQueryRecord);
                } else if (!initialized && !many) {
                    container.put(subSelect.getTransferRelation().getName(), subQueryRecord);
                } else if (!many && initialized) {
                    log.info("Single containment is already set: {}", AsmUtils.getReferenceFQName(subSelect.getTransferRelation()));
                    final Object containmentId = ((Payload) containment).get(getIdentifierProvider().getName());
                    final Object subQueryRecordId = subQueryRecord.get(getIdentifierProvider().getName());
                    if (!Objects.equals(containmentId, subQueryRecordId)) {
                        log.warn("Single containment is already set with different ID: {} != {}", containmentId, subQueryRecordId);
                    }
                } else {
                    throw new IllegalStateException("List is not initialized: " + AsmUtils.getReferenceFQName(subSelect.getTransferRelation()));
                }
            });

            subQueryRecord.remove(subParentKey); // remove parent key from subquery record because it will be added as nested list
        });
    }

    private static EMap<EList<EReference>, Target> getAllTargetPaths(final Target target) {
        final EMap<EList<EReference>, Target> result = ECollections.asEMap(new HashMap<>());
        result.put(ECollections.emptyEList(), target);
        return addReferencesToAllTargetPaths(result, ECollections.emptyEList(), target.getReferencedTargets());
    }

    private static EMap<EList<EReference>, Target> addReferencesToAllTargetPaths(final EMap<EList<EReference>, Target> found, final EList<EReference> path, final EList<ReferencedTarget> processing) {
        if (processing.isEmpty()) {
            return found;
        }

        final Map<EList<EReference>, Target> newTargets = processing.stream()
                .filter(r -> !found.containsValue(r.getTarget()))
                .collect(Collectors.toMap(r -> ECollections.asEList(ImmutableList.<EReference>builder().addAll(path).add(r.getReference()).build()), r -> r.getTarget()));
        found.putAll(newTargets);

        newTargets.entrySet().forEach(nt -> addReferencesToAllTargetPaths(found, nt.getKey(), nt.getValue().getReferencedTargets()));
        return found;
    }

    private static boolean isEmbedded(final EReference reference) {
        return AsmUtils.annotatedAsTrue(reference, "embedded");
    }

    @Builder
    @ToString
    private static class Chunk<ID> {

        Collection<ID> parentIds;

        Collection<ID> instanceIds;
    }
}
