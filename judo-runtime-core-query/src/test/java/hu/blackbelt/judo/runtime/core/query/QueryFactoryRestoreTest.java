package hu.blackbelt.judo.runtime.core.query;

/*-
 * #%L
 * JUDO Runtime Core :: Query
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

import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.meta.expression.DataExpression;
import hu.blackbelt.judo.meta.expression.builder.jql.JqlExpressionBuilder;
import hu.blackbelt.judo.meta.query.Select;
import hu.blackbelt.judo.meta.query.runtime.QueryModel;
import hu.blackbelt.judo.tatami.asm2query.Context;
import hu.blackbelt.judo.tatami.asm2query.EntityTypeExpressions;
import hu.blackbelt.judo.tatami.asm2query.QueryModelPersistence;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EcorePackage;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The restore path of {@link QueryFactory} against the build path, over the Northwind models: the restored factory answers
 * like the built one (TP-19) and its references are the live runtime objects (TP-20).
 */
public class QueryFactoryRestoreTest {

    private static SavedQueryModel saved;
    private static QueryFactory built;
    private static QueryModelPersistence.LoadResult loaded;
    private static QueryFactory restored;

    @BeforeAll
    static void buildSaveAndRestore() throws Exception {
        saved = new SavedQueryModel(NorthwindFixture.create());
        built = saved.built;
        loaded = saved.load();
        restored = QueryFactory.restore(saved.fixture.asmModel, saved.fixture.measureModel, loaded.getQueryModel());
    }

    private static List<EClass> mappedTransferObjectTypes() {
        return saved.fixture.asmUtils.all(EClass.class)
                .filter(saved.fixture.asmUtils::isMappedTransferObjectType)
                .collect(Collectors.toList());
    }

    /**
     * TP-19: every public method of a restored factory answers like the built one, on every mapped type and feature.
     */
    @Test
    void restoredQueryFactoryEqualsBuilt() {
        assertThat(loaded.getUnresolvedProxyCount(), equalTo(0));
        assertThat(built.isRestored(), equalTo(false));
        assertThat(restored.isRestored(), equalTo(true));

        final EcoreUtil.EqualityHelper equality = new EcoreUtil.EqualityHelper();

        // getQuery: equal graphs (the ASM objects they reference are identical), never the built object
        final List<EClass> mapped = mappedTransferObjectTypes();
        assertThat(mapped.size(), greaterThan(0));
        for (EClass c : mapped) {
            final String name = AsmUtils.getClassifierFQName(c);
            assertThat(name, restored.getQuery(c).isPresent(), equalTo(built.getQuery(c).isPresent()));
            if (built.getQuery(c).isPresent()) {
                assertThat(name, restored.getQuery(c).get(), not(sameInstance(built.getQuery(c).get())));
                assertThat(name, equality.equals(built.getQuery(c).get(), restored.getQuery(c).get()), equalTo(true));
            }
        }

        // getNavigation / getDataQuery / isOrdered / isStaticReference / isStaticAttribute for every ASM feature
        int references = 0;
        for (EReference r : saved.fixture.asmUtils.all(EReference.class).collect(Collectors.toList())) {
            references++;
            final String name = AsmUtils.getReferenceFQName(r);
            assertThat(name, restored.getNavigation(r).isPresent(), equalTo(built.getNavigation(r).isPresent()));
            if (built.getNavigation(r).isPresent()) {
                assertThat(name, equality.equals(built.getNavigation(r).get(), restored.getNavigation(r).get()), equalTo(true));
            }
            assertThat(name, restored.isOrdered(r), equalTo(built.isOrdered(r)));
            assertThat(name, restored.isStaticReference(r), equalTo(built.isStaticReference(r)));
        }
        int attributes = 0;
        for (EAttribute a : saved.fixture.asmUtils.all(EAttribute.class).collect(Collectors.toList())) {
            attributes++;
            final String name = AsmUtils.getAttributeFQName(a);
            assertThat(name, restored.getDataQuery(a).isPresent(), equalTo(built.getDataQuery(a).isPresent()));
            if (built.getDataQuery(a).isPresent()) {
                assertThat(name, equality.equals(built.getDataQuery(a).get(), restored.getDataQuery(a).get()), equalTo(true));
            }
            assertThat(name, restored.isStaticAttribute(a), equalTo(built.isStaticAttribute(a)));
        }
        assertThat(references, greaterThan(0));
        assertThat(attributes, greaterThan(0));

        // getNextSourceIndex / getNextTargetIndex
        assertThat(restored.getNextSourceIndex().get(), equalTo(built.getNextSourceIndex().get()));
        assertThat(restored.getNextTargetIndex().get(), equalTo(built.getNextTargetIndex().get()));

        // getEntityTypeExpressionsMap: same entity types, the identical live getter expressions
        assertThat(restored.getEntityTypeExpressionsMap().keySet(), equalTo(built.getEntityTypeExpressionsMap().keySet()));
        assertThat(built.getEntityTypeExpressionsMap().size(), greaterThan(0));
        for (Map.Entry<EClass, EntityTypeExpressions> entry : built.getEntityTypeExpressionsMap().entrySet()) {
            final EntityTypeExpressions other = restored.getEntityTypeExpressionsMap().get(entry.getKey());
            assertThat(other.getGetterAttributeExpressions().keySet(), equalTo(entry.getValue().getGetterAttributeExpressions().keySet()));
            entry.getValue().getGetterAttributeExpressions().forEach((a, e) -> assertThat(other.getGetterAttributeExpressions().get(a), sameInstance(e)));
            assertThat(other.getGetterReferenceExpressions().keySet(), equalTo(entry.getValue().getGetterReferenceExpressions().keySet()));
            entry.getValue().getGetterReferenceExpressions().forEach((r, e) -> assertThat(other.getGetterReferenceExpressions().get(r), sameInstance(e)));
        }

        // getModelAdapter / getQueryModel / getQueryModelResourceSupport
        assertThat(restored.getModelAdapter(), notNullValue());
        assertThat(built.getModelAdapter(), notNullValue());
        assertThat(restored.getQueryModel().getName(), equalTo(built.getQueryModel().getName()));
        assertThat(restored.getQueryModelResourceSupport().isValid(), equalTo(true));
        assertThat(built.getQueryModelResourceSupport().isValid(), equalTo(true));
    }

    /**
     * TP-19: custom joins are a mutable, empty map on the built and on the restored factory; the map is read at build time
     * only, so an entry added afterwards changes nothing.
     */
    @Test
    void customJoinDefinitionsAreMutableAndEmptyOnBothFactories() {
        for (QueryFactory factory : List.of(built, restored)) {
            assertThat(factory.getCustomJoinDefinitions().isEmpty(), equalTo(true));
            final EReference any = saved.fixture.asmUtils.all(EReference.class).findFirst().orElseThrow();
            final int queries = factory.getQuery(mappedTransferObjectTypes().get(0)).isPresent() ? 1 : 0;
            factory.getCustomJoinDefinitions().put(any, CustomJoinDefinition.builder().navigationSql("select 1").build());
            assertThat(factory.getCustomJoinDefinitions().size(), equalTo(1));
            assertThat(factory.getQuery(mappedTransferObjectTypes().get(0)).isPresent() ? 1 : 0, equalTo(queries));
            factory.getCustomJoinDefinitions().clear();
        }
        // a factory built without a map gets a mutable one too
        final QueryFactory withoutMap = QueryFactory.builder()
                .asmResourceSet(saved.fixture.asmModel.getResourceSet())
                .measureResourceSet(saved.fixture.measureModel.getResourceSet())
                .expressionResourceSet(saved.fixture.expressionModel.getResourceSet())
                .build();
        assertThat(withoutMap.getCustomJoinDefinitions().isEmpty(), equalTo(true));
        withoutMap.getCustomJoinDefinitions().put(saved.fixture.asmUtils.all(EReference.class).findFirst().orElseThrow(),
                CustomJoinDefinition.builder().navigationSql("select 1").build());
    }

    /**
     * A restored factory still converts dynamic (request time) expressions: the converter works over the live models, and the new
     * source and target indexes continue after the restored counters.
     */
    @Test
    void restoredFactoryConvertsDynamicExpressions() throws Exception {
        final QueryFactory own = QueryFactory.restore(saved.fixture.asmModel, saved.fixture.measureModel, saved.load().getQueryModel());
        final int sourceBefore = own.getNextSourceIndex().get();
        final int targetBefore = own.getNextTargetIndex().get();
        int converted = 0;
        for (Map.Entry<EClass, EntityTypeExpressions> entry : own.getEntityTypeExpressionsMap().entrySet()) {
            final Select select = mappedTransferObjectTypes().stream()
                    .map(own::getQuery).filter(Optional::isPresent).map(Optional::get)
                    .filter(q -> q.getFrom() == entry.getKey()).findFirst().orElse(null);
            if (select == null) {
                continue;
            }
            for (DataExpression expression : entry.getValue().getGetterAttributeExpressions().values()) {
                final Context context = Context.builder()
                        .createdQueryObjects(new ArrayList<>())
                        .node(select)
                        .sourceCounter(own.getNextSourceIndex())
                        .targetCounter(own.getNextTargetIndex())
                        .variables(Collections.singletonMap(JqlExpressionBuilder.SELF_NAME, select))
                        .build();
                assertThat(own.dataExpressionToFeature(expression, context, null, null), notNullValue());
                converted++;
            }
        }
        assertThat(converted, greaterThan(0));
        assertThat(own.getNextSourceIndex().get(), greaterThanOrEqualTo(sourceBefore));
        assertThat(own.getNextTargetIndex().get(), greaterThanOrEqualTo(targetBefore));
        assertThat(sourceBefore, equalTo(built.getNextSourceIndex().get()));
    }

    /**
     * The entity expressions are served read-only by both factories (callers only read them).
     */
    @Test
    void entityTypeExpressionsMapIsReadOnly() {
        for (QueryFactory factory : List.of(built, restored)) {
            assertThrows(UnsupportedOperationException.class,
                    () -> factory.getEntityTypeExpressionsMap().put(mappedTransferObjectTypes().get(0), null));
        }
    }

    /**
     * TP-20: every non-containment reference of the restored query model that leaves the query resource points to an object
     * that is the very same object as in the runtime's ASM (or expression) resource set; none is a copy or a proxy.
     */
    @Test
    void restoredReferencesAreLiveObjects() {
        final Set<EObject> live = Collections.newSetFromMap(new IdentityHashMap<>());
        saved.fixture.asmModel.getResourceSet().getResources().forEach(r -> r.getAllContents().forEachRemaining(live::add));
        final Set<EObject> liveExpression = Collections.newSetFromMap(new IdentityHashMap<>());
        saved.fixture.expressionModel.getResourceSet().getResources().forEach(r -> r.getAllContents().forEachRemaining(liveExpression::add));

        final QueryModel queryModel = restored.getQueryModel();
        final Resource queryResource = queryModel.getResource();
        int asmTargets = 0;
        int expressionTargets = 0;
        int nonIdentical = 0;
        final TreeIterator<EObject> contents = queryResource.getAllContents();
        while (contents.hasNext()) {
            final EObject object = contents.next();
            for (EReference reference : object.eClass().getEAllReferences()) {
                if (reference.isContainment() || reference.isContainer() || !object.eIsSet(reference)) {
                    continue;
                }
                final Object value = object.eGet(reference, false);
                final List<?> values = value instanceof List ? (List<?>) value : Collections.singletonList(value);
                for (Object v : values) {
                    final EObject target = (EObject) v;
                    if (target.eResource() == queryResource) {
                        continue;
                    }
                    if (target.eIsProxy()) {
                        nonIdentical++;
                    } else if (live.contains(target)) {
                        asmTargets++;
                    } else if (liveExpression.contains(target)) {
                        expressionTargets++;
                    } else if (!isEcore(target)) {
                        nonIdentical++;
                    }
                }
            }
        }
        assertThat("references to a copy or a proxy", nonIdentical, equalTo(0));
        assertThat(asmTargets, greaterThan(0));
        assertThat(expressionTargets, greaterThan(0));

        // the lookups themselves return live ASM objects
        for (EClass c : mappedTransferObjectTypes()) {
            restored.getQuery(c).ifPresent(select -> assertThat(AsmUtils.getClassifierFQName(c), live.contains(select.getFrom()), equalTo(true)));
        }
    }

    private static boolean isEcore(final EObject target) {
        return target instanceof EClassifier && ((EClassifier) target).getEPackage() == EcorePackage.eINSTANCE;
    }
}
