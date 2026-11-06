package hu.blackbelt.judo.runtime.core.query;

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

import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.DataExpression;
import hu.blackbelt.judo.meta.expression.Expression;
import hu.blackbelt.judo.meta.expression.adapters.asm.AsmModelAdapter;
import hu.blackbelt.judo.meta.measure.runtime.MeasureModel;
import hu.blackbelt.judo.meta.measure.support.MeasureModelResourceSupport;
import hu.blackbelt.judo.meta.query.Feature;
import hu.blackbelt.judo.meta.query.Select;
import hu.blackbelt.judo.meta.query.SubSelect;
import hu.blackbelt.judo.meta.query.Target;
import hu.blackbelt.judo.meta.query.runtime.QueryModel;
import hu.blackbelt.judo.meta.query.support.QueryModelResourceSupport;
import hu.blackbelt.judo.tatami.asm2query.Asm2Query;
import hu.blackbelt.judo.tatami.asm2query.Asm2QueryResult;
import hu.blackbelt.judo.tatami.asm2query.Context;
import hu.blackbelt.judo.tatami.asm2query.DataExpressionConverter;
import hu.blackbelt.judo.tatami.asm2query.EntityTypeExpressions;
import hu.blackbelt.judo.tatami.asm2query.QueryIndex;
import hu.blackbelt.judo.tatami.asm2query.feature.ExpressionIterators;
import hu.blackbelt.mapper.api.Coercer;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.resource.ResourceSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Logical query factory for ASM mapped transfer object types.
 *
 * <p>The factory is the runtime entry point of the query model. It has two construction paths:</p>
 * <ul>
 *     <li><b>build</b> (the public constructors and the builder): the logical query model is built from the ASM,
 *     measure and the caller-supplied expression resource sets by {@link Asm2Query} of {@code judo-tatami-asm2query}.</li>
 *     <li><b>restore</b> ({@link #restore(ResourceSet, ResourceSet, QueryModel)}): the factory wraps a query model that was
 *     built at model build time, saved and loaded again (see {@code QueryModelPersistence}); no expression is converted
 *     at boot time.</li>
 * </ul>
 * <p>Use {@link QueryModelLoadOrBuild} to choose between the two paths.</p>
 */
@Slf4j
public class QueryFactory {

    public static final String TABLE_ALIAS_FORMAT = Asm2Query.TABLE_ALIAS_FORMAT;

    public static final String EXPOSED_ALIAS = Asm2Query.EXPOSED_ALIAS;

    public static final String DATA_ALIAS = Asm2Query.DATA_ALIAS;

    private static final String DEFAULT_MODEL_NAME = "model";

    private final QueryIndex queryIndex;

    private final DataExpressionConverter expressionConverter;

    /**
     * Custom joins of the factory. They are consumed only while the query model is built: an entry added later has no
     * effect, and a restored factory always carries an empty (but mutable) map.
     */
    @Getter
    private final Map<EReference, CustomJoinDefinition> customJoinDefinitions;

    /**
     * The (generated or restored) query model.
     */
    @Getter
    private final QueryModel queryModel;

    @Getter
    private final QueryModelResourceSupport queryModelResourceSupport;

    /**
     * {@code true} when the factory wraps a restored (persisted) query model, {@code false} when it was built.
     */
    @Getter
    private final boolean restored;

    /**
     * Build the query model.
     *
     * @param asmResourceSet        ASM resource set
     * @param expressionResourceSet expression resource set the query model is built from
     * @param coercer               ignored
     * @deprecated the coercer is never used; use {@link #QueryFactory(ResourceSet, ResourceSet, ResourceSet, Coercer, Map)}
     * or the builder and pass {@code null}
     */
    @Deprecated
    public QueryFactory(final ResourceSet asmResourceSet, final ResourceSet expressionResourceSet, final Coercer coercer) {
        this(asmResourceSet, MeasureModelResourceSupport.createMeasureResourceSet(), expressionResourceSet, coercer, Collections.emptyMap());
    }

    /**
     * Build the query model.
     *
     * @param asmResourceSet         ASM resource set
     * @param measureResourceSet     measure resource set
     * @param expressionResourceSet  expression resource set the query model is built from
     * @param coercer                ignored
     * @param customJoinDefinitions  custom joins of derived transfer relations, read while the model is built
     */
    @Deprecated
    public QueryFactory(
            @NonNull final ResourceSet asmResourceSet,
            @NonNull final ResourceSet measureResourceSet,
            @NonNull final ResourceSet expressionResourceSet,
            final Coercer coercer,
            final Map<EReference, CustomJoinDefinition> customJoinDefinitions) {
        this(asmResourceSet, measureResourceSet, expressionResourceSet, coercer, customJoinDefinitions, null);
    }

    /**
     * Build the query model.
     *
     * @param asmResourceSet         ASM resource set
     * @param measureResourceSet     measure resource set
     * @param expressionResourceSet  expression resource set the query model is built from
     * @param coercer                ignored (deprecated)
     * @param customJoinDefinitions  custom joins of derived transfer relations, read while the model is built
     * @param modelName              name of the model (the query model gets the logical URI {@code <modelName>-query.model});
     *                               the name of the ASM model when {@code null}
     */
    @Builder
    public QueryFactory(
            @NonNull final ResourceSet asmResourceSet,
            @NonNull final ResourceSet measureResourceSet,
            @NonNull final ResourceSet expressionResourceSet,
            @Deprecated final Coercer coercer,
            final Map<EReference, CustomJoinDefinition> customJoinDefinitions,
            final String modelName) {

        this.customJoinDefinitions = customJoinDefinitions == null ? new ConcurrentHashMap<>() : customJoinDefinitions;

        final Asm2QueryResult result = Asm2Query.executeAsm2Query(Asm2Query.Asm2QueryParameter.asm2QueryParameter()
                .asmResourceSet(asmResourceSet)
                .measureResourceSet(measureResourceSet)
                .expressionResourceSet(expressionResourceSet)
                .modelName(modelName != null ? modelName : modelNameOf(asmResourceSet))
                .customJoinDefinitions(toAsm2QueryJoins(this.customJoinDefinitions)));

        this.queryIndex = result.getIndex();
        this.expressionConverter = result.getExpressionConverter();
        this.queryModel = result.getQueryModel();
        this.queryModelResourceSupport = queryModel.getQueryModelResourceSupport();
        this.restored = false;
    }

    private QueryFactory(final QueryIndex queryIndex, final DataExpressionConverter expressionConverter, final QueryModel queryModel) {
        this.queryIndex = queryIndex;
        this.expressionConverter = expressionConverter;
        this.queryModel = queryModel;
        this.queryModelResourceSupport = queryModel.getQueryModelResourceSupport();
        this.customJoinDefinitions = new ConcurrentHashMap<>();
        this.restored = true;
    }

    /**
     * Restore the factory from a loaded query model.
     *
     * <p>The model (see {@code QueryModelPersistence.load}) must be resolved against the given ASM resource set.
     * The index is rebuilt from its {@code QueryModelInfo} root; no {@code TransferObjectTypeBindingsCollector} and no
     * expression evaluation is needed. Dynamic filters are still supported. Custom joins are never applied.</p>
     *
     * @param asmResourceSet     live ASM resource set the references of the query model point into
     * @param measureResourceSet live measure resource set (optional)
     * @param queryModel         loaded query model
     * @return restored factory
     * @throws IllegalStateException if the query model is invalid or its info cannot be restored
     */
    public static QueryFactory restore(@NonNull final ResourceSet asmResourceSet, final ResourceSet measureResourceSet,
                                       @NonNull final QueryModel queryModel) {
        final QueryIndex index = QueryIndex.restore(asmResourceSet, queryModel);
        if (!queryModel.isValid()) {
            log.error("Invalid query model: {}", queryModel.getDiagnosticsAsString());
            throw new IllegalStateException("Invalid query model");
        }
        final DataExpressionConverter expressionConverter = new DataExpressionConverter(asmResourceSet, measureResourceSet);
        createGetterIterators(index, expressionConverter.getModelAdapter());
        return new QueryFactory(index, expressionConverter, queryModel);
    }

    /**
     * A build leaves the iterator variables of the getter collection expressions in the expression model; the saved expression
     * model has none. The DAO converts detached copies of the getters (dynamic filters), and a copy cannot get an iterator
     * created (it has no resource), so the restore creates them like the build did. Runs at boot, single-threaded, before the
     * factory is published.
     */
    private static void createGetterIterators(final QueryIndex index, final AsmModelAdapter modelAdapter) {
        final List<Expression> getters = new ArrayList<>();
        for (EntityTypeExpressions expressions : index.getEntityTypeExpressionsMap().values()) {
            getters.addAll(expressions.getGetterAttributeExpressions().values());
            getters.addAll(expressions.getGetterReferenceExpressions().values());
        }
        final int created = ExpressionIterators.createMissing(modelAdapter, getters);
        log.debug("Restored the iterators of the getter expressions: {} created", created);
    }

    /**
     * Restore the factory from a loaded query model.
     *
     * @param asmModel     live ASM model
     * @param measureModel live measure model (optional)
     * @param queryModel   loaded query model
     * @return restored factory
     * @see #restore(ResourceSet, ResourceSet, QueryModel)
     */
    public static QueryFactory restore(@NonNull final AsmModel asmModel, final MeasureModel measureModel,
                                       @NonNull final QueryModel queryModel) {
        return restore(asmModel.getResourceSet(), measureModel != null ? measureModel.getResourceSet() : null, queryModel);
    }

    private static String modelNameOf(final ResourceSet asmResourceSet) {
        return new AsmUtils(asmResourceSet).getModel().map(EPackage::getName).orElse(DEFAULT_MODEL_NAME);
    }

    private static Map<EReference, hu.blackbelt.judo.tatami.asm2query.CustomJoinDefinition> toAsm2QueryJoins(
            final Map<EReference, CustomJoinDefinition> joins) {
        final Map<EReference, hu.blackbelt.judo.tatami.asm2query.CustomJoinDefinition> result = new ConcurrentHashMap<>();
        joins.forEach((reference, join) -> {
            if (reference != null && join != null) {
                result.put(reference, hu.blackbelt.judo.tatami.asm2query.CustomJoinDefinition.builder()
                        .sourceIdParameterName(join.getSourceIdParameterName())
                        .sourceIdSetParameterName(join.getSourceIdSetParameterName())
                        .navigationSql(join.getNavigationSql())
                        .build());
            }
        });
        return result;
    }

    public AtomicInteger getNextSourceIndex() {
        return queryIndex.getNextSourceIndex();
    }

    public AtomicInteger getNextTargetIndex() {
        return queryIndex.getNextTargetIndex();
    }

    public AsmModelAdapter getModelAdapter() {
        return expressionConverter.getModelAdapter();
    }

    /**
     * @return the getter expressions of every entity type (read-only view)
     */
    public Map<EClass, EntityTypeExpressions> getEntityTypeExpressionsMap() {
        return queryIndex.getEntityTypeExpressionsMap();
    }

    /**
     * Get logical query of a given transfer object type.
     * Logical query contains all information required to create SQL SELECT statement on ASM and no SQL table and column names and navigation (ID, foreign key name) is resolved.
     *
     * @param transferObjectType mapped transfer object type
     * @return logical query
     */
    public Optional<Select> getQuery(final EClass transferObjectType) {
        return queryIndex.getQuery(transferObjectType);
    }

    public Optional<SubSelect> getNavigation(final EReference navigation) {
        return queryIndex.getNavigation(navigation);
    }

    public Optional<SubSelect> getDataQuery(final EAttribute attribute) {
        return queryIndex.getDataQuery(attribute);
    }

    public boolean isOrdered(final EReference transferObjectRelation) {
        return queryIndex.isOrdered(transferObjectRelation);
    }

    public Feature dataExpressionToFeature(final DataExpression dataExpression, final Context context, final Target target, final EAttribute targetAttribute) {
        return expressionConverter.dataExpressionToFeature(dataExpression, context, target, targetAttribute);
    }

    public boolean isStaticReference(final EReference reference) {
        return queryIndex.isStaticReference(reference);
    }

    public boolean isStaticAttribute(final EAttribute attribute) {
        return queryIndex.isStaticAttribute(attribute);
    }
}
