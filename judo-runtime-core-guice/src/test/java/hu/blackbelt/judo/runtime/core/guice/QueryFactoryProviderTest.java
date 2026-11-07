package hu.blackbelt.judo.runtime.core.guice;

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

import ch.qos.logback.classic.Level;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import hu.blackbelt.judo.meta.expression.builder.jql.asm.AsmJqlExtractor;
import hu.blackbelt.judo.runtime.core.guice.ModelSources.Source;
import hu.blackbelt.judo.runtime.core.guice.ModelSources.State;
import hu.blackbelt.judo.runtime.core.guice.dao.rdbms.QueryFactoryProvider;
import hu.blackbelt.judo.runtime.core.query.QueryFactory;
import hu.blackbelt.judo.runtime.core.query.QueryModelLoadOrBuild;
import hu.blackbelt.mapper.api.Coercer;
import hu.blackbelt.mapper.impl.DefaultCoercer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tasks 1.5 and 2.3 (TP-23): {@link QueryFactoryProvider} decides through {@link QueryModelLoadOrBuild} for a directory, a
 * classpath jar and a file URL, with the query model present, absent, corrupt and stale, in every mode.
 */
class QueryFactoryProviderTest {

    @AfterAll
    static void cleanUp() {
        ModelSources.cleanUp();
    }

    /** What {@link #provide} saw: the factory or the failure, and the log of the decision. */
    private static final class Outcome {
        QueryFactory queryFactory;
        RuntimeException failure;
        List<String> info;
        List<String> warnings;
        int extractors;

        String failureText() {
            final StringBuilder text = new StringBuilder();
            for (Throwable t = failure; t != null; t = t.getCause()) {
                text.append(t.getMessage()).append('\n');
            }
            return text.toString();
        }
    }

    private static Outcome provide(final JudoModelLoader loader, final String mode) {
        final Outcome outcome = new Outcome();
        try (LogCapture log = new LogCapture(QueryModelLoadOrBuild.class);
             MockedConstruction<AsmJqlExtractor> extractors = Mockito.mockConstruction(AsmJqlExtractor.class,
                     // a spy of the JQL extraction: counts the instances, extracts nothing
                     (mock, context) -> Mockito.when(mock.extractExpressions()).thenThrow(new ExtractorInvoked()))) {
            final Injector injector = Guice.createInjector(new AbstractModule() {
                @Override
                protected void configure() {
                    bind(JudoModelLoader.class).toInstance(loader);
                    bind(Coercer.class).toInstance(new DefaultCoercer());
                    if (mode != null) {
                        bind(String.class).annotatedWith(JudoConfigurationQualifiers.QueryModelMode.class).toInstance(mode);
                    }
                    bind(QueryFactory.class).toProvider(QueryFactoryProvider.class).in(Singleton.class);
                }
            });
            try {
                outcome.queryFactory = injector.getInstance(QueryFactory.class);
            } catch (RuntimeException e) {
                outcome.failure = e;
            }
            outcome.extractors = extractors.constructed().size();
            outcome.info = log.only(Level.INFO);
            outcome.warnings = log.warnings();
        }
        return outcome;
    }

    /** The build path would extract the JQL; here it does not matter what is built, only that it was asked for. */
    private static final class ExtractorInvoked extends RuntimeException {
        ExtractorInvoked() {
            super("JQL extraction invoked");
        }
    }

    @TestFactory
    List<DynamicTest> matrix() {
        final List<DynamicTest> tests = new ArrayList<>();
        final Map<String, JudoModelLoader> loaders = new HashMap<>();
        for (Source source : Source.values()) {
            for (State state : State.values()) {
                for (String mode : new String[]{"auto", "load", "build", null}) {
                    tests.add(DynamicTest.dynamicTest(source + " / " + state + " / " + (mode == null ? "default" : mode), () -> {
                        final JudoModelLoader loader = loaders.computeIfAbsent(source + "/" + state, k -> {
                            try {
                                return ModelSources.load(source, state);
                            } catch (Exception e) {
                                throw new IllegalStateException(e);
                            }
                        });
                        verify(provide(loader, mode), state, mode == null ? "auto" : mode);
                    }));
                }
            }
        }
        return tests;
    }

    private static void verify(final Outcome outcome, final State state, final String mode) {
        if ("build".equals(mode)) {
            // build never opens the artifact and takes the provider's existing JQL extraction
            assertThat(outcome.extractors, equalTo(1));
            assertThat(outcome.failureText(), containsString("JQL extraction invoked"));
            assertThat(outcome.info, not(hasItem(containsString("restored"))));
            assertThat(outcome.warnings, empty());
            return;
        }
        if (state == State.PRESENT) {
            assertThat(outcome.failureText(), outcome.failure == null, equalTo(true));
            assertThat(outcome.queryFactory.isRestored(), equalTo(true));
            assertThat("the JQL extractor is not invoked for the query model", outcome.extractors, equalTo(0));
            assertThat(outcome.info, hasItem(containsString("query model restored")));
            assertThat(outcome.info, hasItem(containsString(" ms")));
            assertThat(outcome.warnings, empty());
            return;
        }
        if ("load".equals(mode)) {
            assertThat("strict load fails", outcome.failure != null, equalTo(true));
            assertThat("the JQL extractor is not invoked", outcome.extractors, equalTo(0));
            assertThat(outcome.failureText(), containsString(state == State.ABSENT ? "missing"
                    : state == State.STALE ? "expressionDigest mismatch" : "Query model mode is load"));
            return;
        }
        // auto: the existing build runs and the injector starts
        assertThat(outcome.extractors, equalTo(1));
        assertThat(outcome.failureText(), containsString("JQL extraction invoked"));
        if (state == State.ABSENT) {
            assertThat("absent is the normal case, no warning", outcome.warnings, empty());
        } else {
            assertThat(outcome.warnings, hasSize(1));
            assertThat(outcome.warnings.get(0), containsString(state == State.STALE ? "expressionDigest mismatch" : "Persisted query model"));
        }
    }

    @TestFactory
    List<DynamicTest> tp23RestoredFromDirectoryClasspathAndUrl() {
        final List<DynamicTest> tests = new ArrayList<>();
        for (Source source : Source.values()) {
            tests.add(DynamicTest.dynamicTest(source.toString(), () -> {
                final Outcome outcome = provide(ModelSources.load(source, State.PRESENT), "auto");

                assertThat(outcome.failureText(), outcome.failure == null, equalTo(true));
                assertThat(outcome.queryFactory.isRestored(), equalTo(true));
                assertThat("INFO line with the duration", outcome.info, hasItem(containsString("query model restored")));
                assertThat("0 unresolved: a restore with unresolved proxies would fall back with a WARN", outcome.warnings, empty());
                assertThat("spy count 0", outcome.extractors, equalTo(0));
            }));
        }
        return tests;
    }
}
