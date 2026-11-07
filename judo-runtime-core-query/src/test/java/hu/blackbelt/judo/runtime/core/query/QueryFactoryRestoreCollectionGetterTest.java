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

import hu.blackbelt.judo.meta.expression.DataExpression;
import hu.blackbelt.judo.meta.expression.CollectionExpression;
import hu.blackbelt.judo.meta.expression.builder.jql.JqlExpressionBuilder;
import hu.blackbelt.judo.meta.expression.constant.Instance;
import hu.blackbelt.judo.meta.expression.numeric.CountExpression;
import hu.blackbelt.judo.meta.expression.object.ObjectVariableReference;
import hu.blackbelt.judo.meta.query.Feature;
import hu.blackbelt.judo.meta.query.Select;
import hu.blackbelt.judo.tatami.asm2query.Context;
import hu.blackbelt.judo.tatami.asm2query.EntityTypeExpressions;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Dynamic filters over a derived entity attribute whose getter contains a collection (count) expression, on a restored
 * {@link QueryFactory}.
 *
 * <p>The DAO converts a <em>detached copy</em> of the getter expression ({@code AttributeSelectorTranslator}). After a build the
 * getter expressions carry their iterator variables (the build attaches them), so the copy inherits them. The saved expression
 * model has no iterators, so the restore must create them (see {@code ExpressionIterators}); otherwise the converter would have
 * to create the iterator on a copy that has no resource.</p>
 * See change: persisted-query-model
 */
public class QueryFactoryRestoreCollectionGetterTest {

    private static final class GetterCase {
        final EClass entityType;
        final EAttribute attribute;

        GetterCase(final EClass entityType, final EAttribute attribute) {
            this.entityType = entityType;
            this.attribute = attribute;
        }

        @Override
        public String toString() {
            return entityType.getName() + "." + attribute.getName();
        }
    }

    private static SavedQueryModel saved;
    private static QueryFactory restored;
    private static Map<GetterCase, Feature> expected;
    private static List<GetterCase> cases;

    /**
     * Built factory first (the expressions get their iterators and the copies inherit them: the expected features), then the
     * iterators are taken off the expressions (what the saved expression model looks like) and the factory is restored.
     */
    @BeforeAll
    static void buildConvertThenRestoreWithoutIterators() throws Exception {
        final NorthwindFixture fixture = NorthwindFixture.create();

        // the collections without an iterator in the expression model as it is saved: the build creates theirs lazily and keeps
        // them; the iterators of the other ones (lambda variables) are part of the model and referenced by the lambda bodies
        final Set<CollectionExpression> withoutIterator = Collections.newSetFromMap(new IdentityHashMap<>());
        fixture.expressionModel.getResourceSet().getResources().forEach(resource ->
                resource.getAllContents().forEachRemaining(o -> {
                    if (o instanceof CollectionExpression && ((CollectionExpression) o).getIteratorVariable() == null) {
                        withoutIterator.add((CollectionExpression) o);
                    }
                }));
        assertThat(withoutIterator.size(), greaterThan(0));

        saved = new SavedQueryModel(fixture);

        cases = new ArrayList<>();
        for (Map.Entry<EClass, EntityTypeExpressions> entry : saved.built.getEntityTypeExpressionsMap().entrySet()) {
            entry.getValue().getGetterAttributeExpressions().forEach((attribute, expression) -> {
                if (containsCount(expression)) {
                    cases.add(new GetterCase(entry.getKey(), attribute));
                }
            });
        }

        expected = new LinkedHashMap<>();
        for (GetterCase getterCase : cases) {
            expected.put(getterCase, convertCopy(saved.built, getterCase));
        }

        // back to the expression model as it is saved (the iterators the build created are not part of it)
        withoutIterator.forEach(collection -> collection.setIteratorVariable(null));

        restored = QueryFactory.restore(saved.fixture.asmModel, saved.fixture.measureModel, saved.load().getQueryModel());
    }

    private static boolean containsCount(final DataExpression expression) {
        if (expression instanceof CountExpression) {
            return true;
        }
        for (TreeIterator<EObject> it = expression.eAllContents(); it.hasNext(); ) {
            if (it.next() instanceof CountExpression) {
                return true;
            }
        }
        return false;
    }

    /**
     * Exactly the steps of {@code AttributeSelectorTranslator}: copy the getter, point its {@code self} references to the variable
     * of the filter, convert the copy.
     */
    private static Feature convertCopy(final QueryFactory factory, final GetterCase getterCase) {
        final DataExpression getter = factory.getEntityTypeExpressionsMap().get(getterCase.entityType)
                .getGetterAttributeExpressions().get(getterCase.attribute);
        // a conversion adds its sub-selects to the node: convert on a clean copy of the select, never on the shared one
        final Select select = EcoreUtil.copy(saved.fixture.asmUtils.all(EClass.class)
                .filter(saved.fixture.asmUtils::isMappedTransferObjectType)
                .map(factory::getQuery).filter(Optional::isPresent).map(Optional::get)
                .filter(q -> q.getFrom() == getterCase.entityType).findFirst().orElseThrow());

        final DataExpression copy = EcoreUtil.copy(getter);
        assertThat(copy.eResource(), nullValue());
        final List<ObjectVariableReference> selfReferences = new ArrayList<>();
        collectSelfReferences(copy, selfReferences);
        assertThat(selfReferences.size(), greaterThan(0));
        for (ObjectVariableReference selfReference : selfReferences) {
            selfReference.setVariable(EcoreUtil.copy((Instance) selfReference.getVariable()));
        }

        final Context context = Context.builder()
                .createdQueryObjects(new ArrayList<>())
                .node(select)
                .sourceCounter(new AtomicInteger(factory.getNextSourceIndex().get()))
                .targetCounter(new AtomicInteger(factory.getNextTargetIndex().get()))
                .variables(Collections.singletonMap(JqlExpressionBuilder.SELF_NAME, select))
                .build();
        return factory.dataExpressionToFeature(copy, context, null, null);
    }

    private static void collectSelfReferences(final EObject root, final List<ObjectVariableReference> target) {
        if (isSelfReference(root)) {
            target.add((ObjectVariableReference) root);
        }
        root.eAllContents().forEachRemaining(o -> {
            if (isSelfReference(o)) {
                target.add((ObjectVariableReference) o);
            }
        });
    }

    private static boolean isSelfReference(final EObject o) {
        return o instanceof ObjectVariableReference
                && JqlExpressionBuilder.SELF_NAME.equals(((ObjectVariableReference) o).getVariableName())
                && ((ObjectVariableReference) o).getVariable() instanceof Instance;
    }

    /**
     * Equality of the query graphs that ignores the generated aliases (they depend on the counters, not on the structure).
     */
    private static final class IgnoringAliases extends EcoreUtil.EqualityHelper {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean haveEqualAttribute(final EObject eObject1, final EObject eObject2, final org.eclipse.emf.ecore.EAttribute attribute) {
            return "alias".equals(attribute.getName()) || super.haveEqualAttribute(eObject1, eObject2, attribute);
        }
    }

    /**
     * Dynamic filter over a collection getter after restore: the copy of the getter converts, and to the structure a built
     * factory gives for the same input.
     */
    @Test
    void dynamicFilterOverCollectionGetterAfterRestore() {
        assertThat("Northwind has derived attributes with a count getter", cases.size(), greaterThan(0));
        for (GetterCase getterCase : cases) {
            final Feature actual = convertCopy(restored, getterCase);
            assertThat(getterCase.toString(), actual, notNullValue());
            assertThat(getterCase.toString(), new IgnoringAliases().equals(expected.get(getterCase), actual), equalTo(true));
        }
    }

    /**
     * The restore created the iterators of the getter collections, so the copy does not depend on creating one.
     */
    @Test
    void restoreCreatedTheIteratorsOfTheGetterCollections() {
        int collections = 0;
        for (GetterCase getterCase : cases) {
            final DataExpression getter = restored.getEntityTypeExpressionsMap().get(getterCase.entityType)
                    .getGetterAttributeExpressions().get(getterCase.attribute);
            for (TreeIterator<EObject> it = getter.eAllContents(); it.hasNext(); ) {
                final EObject o = it.next();
                if (o instanceof CollectionExpression) {
                    collections++;
                    assertThat(getterCase.toString(), ((CollectionExpression) o).getIteratorVariable(), notNullValue());
                }
            }
        }
        assertThat(collections, greaterThan(0));
    }
}
