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

import hu.blackbelt.judo.meta.query.CustomJoin;
import hu.blackbelt.mapper.api.Coercer;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * The build path of {@link QueryFactory}: delegation to {@code Asm2Query} keeps the behaviour of the former builder.
 */
public class QueryFactoryBuildTest {

    private static final NorthwindFixture fixture = NorthwindFixture.create();

    /**
     * The deprecated {@code coercer} argument is accepted and never called.
     */
    @Test
    @SuppressWarnings("deprecation")
    void coercerIsIgnored() {
        final AtomicInteger calls = new AtomicInteger();
        final Coercer coercer = (Coercer) Proxy.newProxyInstance(Coercer.class.getClassLoader(), new Class<?>[]{Coercer.class},
                (proxy, method, args) -> {
                    calls.incrementAndGet();
                    return null;
                });

        final QueryFactory factory = new QueryFactory(fixture.asmModel.getResourceSet(), fixture.measureModel.getResourceSet(),
                fixture.expressionModel.getResourceSet(), coercer, new HashMap<>());
        final QueryFactory built = QueryFactory.builder()
                .asmResourceSet(fixture.asmModel.getResourceSet())
                .measureResourceSet(fixture.measureModel.getResourceSet())
                .expressionResourceSet(fixture.expressionModel.getResourceSet())
                .coercer(coercer)
                .build();

        assertThat(calls.get(), equalTo(0));
        for (QueryFactory f : List.of(factory, built)) {
            assertThat(f.isRestored(), equalTo(false));
            assertThat(f.getQueryModelResourceSupport().isValid(), equalTo(true));
            assertThat(f.getQueryModel().getName(), equalTo(fixture.asmModel.getName()));
        }
    }

    /**
     * Custom joins are copied field for field into the builder, are applied while the model is built, and an entry added
     * afterwards changes nothing.
     */
    @Test
    void customJoinsAreAppliedAtBuildTimeOnly() {
        final Map<EReference, CustomJoinDefinition> joins = new HashMap<>();
        fixture.asmUtils.all(EReference.class)
                .filter(EReference::isDerived)
                .forEach(r -> joins.put(r, CustomJoinDefinition.builder()
                        .navigationSql("select * from custom")
                        .sourceIdParameterName("sourceId")
                        .sourceIdSetParameterName("sourceIds")
                        .build()));
        assertThat(joins.size(), greaterThan(0));

        final QueryFactory factory = QueryFactory.builder()
                .asmResourceSet(fixture.asmModel.getResourceSet())
                .measureResourceSet(fixture.measureModel.getResourceSet())
                .expressionResourceSet(fixture.expressionModel.getResourceSet())
                .customJoinDefinitions(joins)
                .build();

        final List<CustomJoin> customJoins = customJoinsOf(factory);
        assertThat(customJoins.size(), greaterThan(0));
        for (CustomJoin join : customJoins) {
            assertThat(join.getNavigationSql(), equalTo("select * from custom"));
            assertThat(join.getSourceIdParameter(), equalTo("sourceId"));
            assertThat(join.getSourceIdSetParameter(), equalTo("sourceIds"));
        }

        // the caller's map is the one the factory returns; adding after the build changes nothing
        assertThat(factory.getCustomJoinDefinitions() == joins, equalTo(true));
        final int before = customJoinsOf(factory).size();
        fixture.asmUtils.all(EReference.class).filter(r -> !joins.containsKey(r))
                .forEach(r -> factory.getCustomJoinDefinitions().put(r, CustomJoinDefinition.builder().navigationSql("select 2").build()));
        assertThat(customJoinsOf(factory), hasSize(before));

        // the same model without custom joins has none
        final QueryFactory plain = QueryFactory.builder()
                .asmResourceSet(fixture.asmModel.getResourceSet())
                .measureResourceSet(fixture.measureModel.getResourceSet())
                .expressionResourceSet(fixture.expressionModel.getResourceSet())
                .build();
        assertThat(customJoinsOf(plain), hasSize(0));
    }

    private static List<CustomJoin> customJoinsOf(final QueryFactory factory) {
        return StreamSupport.stream(((Iterable<EObject>) () -> factory.getQueryModel().getResource().getAllContents()).spliterator(), false)
                .filter(CustomJoin.class::isInstance)
                .map(CustomJoin.class::cast)
                .collect(Collectors.toList());
    }
}
